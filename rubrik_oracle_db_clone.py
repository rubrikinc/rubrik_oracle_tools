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
@click.option('--keyfile', '-k', type=str, required=False,  help='The connection keyfile path')
@click.option('--insecure', is_flag=True,  help='Flag to use insecure connection')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL.')
def cli(source_host_db, host_target, time_restore, new_name, pfile, aco_file_path, oracle_home, wait, wait_time, keyfile, insecure, debug_level):
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

    rubrik = rbs_oracle_common.RubrikConnection(keyfile, insecure)
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
    if pfile:
        logger.warning("Using custom PFILE File: {}.".format(pfile))
        if aco_parameters:
            logger.debug("ACO Parameters: {0}".format(aco_parameters))
            for config in aco_parameters:
                if config[0].upper() != 'ORACLE_HOME' and config[0].upper() != 'SPFILE_LOCATION':
                    rubrik.delete_session()
                    raise RubrikOracleDBCloneError("When using a custom PFILE the only parameters allowed in the ACO "
                                                   "file are ORACLE_HOME and SPFILE_LOCATION.")
    if new_name:
        logger.debug("Using new_name: {0}".format(new_name))
        target_name = new_name
    else:
        target_name = database.database_name
    logger.warning("Starting Clone of {0} to {1} on {2}".format(database.database_name, target_name, host_target))
    logger.debug("db_clone parameters host_id={0}, time_ms={1}, new_name={2}, pfile={3}, aco_parameters={4} oracle_home={5}".format(host_id, time_ms, new_name, pfile, aco_parameters, oracle_home))
    db_clone_info = database.db_clone(host_id=host_id, time_ms=time_ms, new_name=new_name, pfile=pfile, aco_parameters=aco_parameters, oracle_home=oracle_home)
    logger.debug(db_clone_info)
    cluster_timezone = pytz.timezone(rubrik.timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(db_clone_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    logger.debug("Database clone status: {0}, Started at {1}.".format(db_clone_info['status'], start_time.strftime(fmt)))
    if not wait:
        rubrik.delete_session()
        return db_clone_info
    else:
        db_clone_info = database.async_requests_wait(db_clone_info['id'], wait_time)
        logger.warning("Async request completed with status: {}".format(db_clone_info['status']))
        if db_clone_info['status'] != "SUCCEEDED":
            rubrik.delete_session()
            raise RubrikOracleDBCloneError(
                "Clone of Oracle DB did not complete successfully. Clone ended with status {}".format(
                    db_clone_info['status']))
        logger.warning("Clone of the database has completed.")
        rubrik.delete_session()
        return db_clone_info


class RubrikOracleDBCloneError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
