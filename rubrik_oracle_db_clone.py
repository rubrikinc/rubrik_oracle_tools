import rbs_oracle_common
import click
import logging
import sys
import datetime
import pytz
import base64
from configparser import ConfigParser


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--host_target', '-h', type=str, required=True, help='Host or RAC cluster name (RAC target required if source is RAC)  for the Live Mount ')
@click.option('--time_restore', '-t', type=str, help='Point in time to mount the DB, iso format is YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15')
@click.option('--new_name', '-n', type=str, help='Name for cloned database')
@click.option('--pfile', '-p', type=str, help='Custom Pfile path (on target host)')
@click.option('--aco_file_path', '-a', type=str, help='ACO file path for parameter changes')
@click.option('--oracle_home', '-o', type=str, help='ORACLE_HOME on destination host. Required as option or in ACO File if source is a Data Guard Group.')
@click.option('--wait', is_flag=True, help='Wait for clone to complete. Times out at wait time.')
@click.option('--wait_time', type=str, default=1800, help='Time for script to wait for clone to complete. Script exits but clone continues at time out.')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL.')
def cli(source_host_db, host_target, time_restore, new_name, pfile, aco_file_path, oracle_home, wait, wait_time, debug_level):
    """Clones an Oracle Database (alternate host restore or duplicate).

     Initiates an Oracle DB clone using the Rubrik RBS automated clone. This can be run on any host since clone will
    be initialed on the host_target provided. Changing the the name with the new_name parameter requires an ACO file 
    or a custom pfile with the following sets of parameters specified:

   \b
   (a) db_file_name_convert, log_file_name_convert, parameter_value_convert
   (b) control_files, db_create_file_dest

    If time restore is not specified, the restore time will be to the latest recovery point on Rubrik. The script will
    initiate the clone and exit unless --wait is specified. Then the script will monitor the async request for the
    wait time (default 30 min.)

    \b
    Returns:
      db_clone_info (json); JSON text file with the Rubrik cluster response to the database clone request

    """
    numeric_level = getattr(logging, debug_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: {}'.format(debug_level))
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(numeric_level)
    console_formatter = logging.Formatter('%(asctime)s: %(message)s')
    ch.setFormatter(console_formatter)
    logger.addHandler(ch)

    rubrik = rbs_oracle_common.RubrikConnection()
    source_host_db = source_host_db.split(":")
    database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, source_host_db[1], source_host_db[0])
    oracle_db_info = database.get_oracle_db_info()
    logger.debug(oracle_db_info)
    # If source DB is RAC then the target for the live mount must be a RAC cluster
    host_id = None
    if 'racName' in oracle_db_info.keys():
        if oracle_db_info['racName']:
            host_id = database.get_rac_id(rubrik.cluster_id, host_target)
    else:
        host_id = database.get_host_id(rubrik.cluster_id, host_target)
    if time_restore:
        time_ms = database.epoch_time(time_restore, rubrik.timezone)
        logger.warning("Using {} for mount.". format(time_restore))
    else:
        logger.warning("Using most recent recovery point for mount.")
        time_ms = database.epoch_time(oracle_db_info['latestRecoveryPoint'], rubrik.timezone)
    aco_config = None
    aco_parameters = None
    base64_aco_file = None
    if aco_file_path:
        logger.warning("Using ACO File: {}".format(aco_file_path))
        aco_config = ConfigParser()
        with open(aco_file_path) as f:
            aco_config.read_string('[ACO]' + f.read())
        logger.debug("ACO Config: {0}".format(aco_config.items('ACO')))
        aco_parameters = aco_config.items('ACO')
        if not database.v6:
            try:
                aco_file = open(aco_file_path, "r").read()
            except IOError as e:
                raise RubrikOracleDBCloneError("I/O error({0}): {1}".format(e.errno, e.strerror))
            except Exception:
                raise RubrikOracleDBCloneError("Unexpected error: {}".format(sys.exc_info()[0]))
            base64_aco_byte_file = base64.b64encode(aco_file.encode("utf-8"))
            base64_aco_file = str(base64_aco_byte_file, "utf-8")
    if pfile:
        logger.warning("Using custom PFILE File: {}.".format(pfile))
        if aco_parameters:
            logger.debug("ACO Parameters: {0}".format(aco_parameters))
            for config in aco_parameters:
                if config[0].upper() != 'ORACLE_HOME' and config[0].upper() != 'SPFILE_LOCATION':
                    raise RubrikOracleDBCloneError("When using a custom PFILE the only parameters allowed in the ACO "
                                                   "file are ORACLE_HOME and SPFILE_LOCATION.")
    logger.debug("dataGuardType is {0}".format(oracle_db_info['dataGuardType']))
    # If source is a Data Guard Group, check to be sure an ORACLE_HOME is provided
    if oracle_db_info['dataGuardType'] == 'DataGuardGroup':
        if oracle_home:
            logger.debug("DG GROUP USING ORACLE_HOME OPTION")
        elif aco_config:
            logger.debug("Source is a DG Group and ACO File is being used. Checking for ORACLE_HOME...")
            if aco_config.has_option('ACO','ORACLE_HOME'):
                logger.debug("ORACLE_HOME: {0} is present in the ACO File.".format(aco_config.get('ACO','ORACLE_HOME')))
                oracle_home = aco_config.get('ACO', 'ORACLE_HOME')
            else:
                logger.warning("ORACLE_HOME is not set in the ACO File: {0} or provided as an option.".format(aco_file_path))
                raise RubrikOracleDBCloneError("When cloning a DG Group database, the ORACLE_HOME must be provided")
        else:
            logger.warning("ORACLE_HOME must be specified for a DG Group.")
            raise RubrikOracleDBCloneError("When cloning a DG Group database, the ORACLE_HOME must be provided")
    if oracle_home and database.v6:
        logger.debug("ORACLE_HOME is {0}".format(oracle_home))
    elif oracle_home:
        raise RubrikOracleDBCloneError("The Oracle Home parameter is not supported with pre 6.0 CDM.")
    if new_name:
        logger.debug("Using new_name: {0}".format(new_name))
        if aco_config:
            if all(key.lower() in aco_config['ACO'] for key in ('parameter_value_convert', 'db_file_name_convert', 'log_file_name_convert')):
                logger.debug("Using a new database name with an ACO file. Required parameters, "
                             "parameter_value_convert, db_file_name_convert, and log_file_name_convert are present")
            elif all(key.lower() in aco_config['ACO'] for key in ('control_files', 'db_create_file_dest')):
                logger.debug("Using a new database name with an ACO file. Required parameters control_files and "
                             "db_create_file_dest are present.")
            else:
                raise RubrikOracleDBCloneError("Using a new database name requires either an ACO file or a custom "
                                               "pfile with db_file_name_convert, log_file_name_convert, "
                                               "parameter_value_convert or control_files, db_create_file_dest")
        elif pfile:
            logger.warning("Using a new database name with a custom pfile. Required parameters, "
                           "parameter_value_convert, db_file_name_convert, and log_file_name_convert or control_files "
                           "and db_create_file_dest.")
        else:
            raise RubrikOracleDBCloneError("Using a new database name requires either an ACO file or a custom pfile. "
                                           "The following parameters are required: db_file_name_convert, "
                                           "log_file_name_convert, parameter_value_convert or control_files, "
                                           "db_create_file_dest.")
        target_name = new_name
    else:
        target_name = database.database_name
    logger.warning("Starting Clone of {0} to {1} on {2}".format(database.database_name, target_name, host_target))
    logger.debug("db_clone parameters host_id={0}, time_ms={1}, new_name={2}, pfile={3}, aco_file={4}, "
                 "aco_parameters={5} oracle_home={6}".format(host_id, time_ms, new_name, pfile, aco_file_path,
                                                             aco_parameters, oracle_home))
    db_clone_info = database.db_clone(host_id=host_id, time_ms=time_ms, new_name=new_name, pfile=pfile, aco_file=base64_aco_file, aco_parameters=aco_parameters, oracle_home=oracle_home)
    logger.debug(db_clone_info)
    cluster_timezone = pytz.timezone(rubrik.timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(db_clone_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    logger.debug("Database clone status: {0}, Started at {1}.".format(db_clone_info['status'], start_time.strftime(fmt)))
    if not wait:
        return db_clone_info
    else:
        db_clone_info = database.async_requests_wait(db_clone_info['id'], wait_time)
        logger.warning("Async request completed with status: {}".format(db_clone_info['status']))
        if db_clone_info['status'] != "SUCCEEDED":
            raise RubrikOracleDBCloneError(
                "Clone of Oracle DB did not complete successfully. Clone ended with status {}".format(
                    db_clone_info['status']))
        logger.warning("Clone of the database has completed.")
        return db_clone_info


class RubrikOracleDBCloneError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
