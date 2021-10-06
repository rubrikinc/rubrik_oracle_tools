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
@click.option('--pfile', '-p', type=str, help='Custom Pfile path (on target host)')
@click.option('--aco_file_path', '-a', type=str, help='ACO file path for parameter changes')
@click.option('--oracle_home', '-o', type=str, help='ORACLE_HOME on destination host. Required as option or in ACO File if source is a Data Guard Group.')
@click.option('--no_wait', is_flag=True, help='Queue Live Mount and exit.')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL.')
def cli(source_host_db, host_target, time_restore, pfile, aco_file_path, oracle_home, no_wait, debug_level):
    """Live mount a Rubrik Oracle Backup.

\b
    Gets the backup for the Oracle database on the Oracle database host and will live mount it on the host provided.

\b
    Returns:
        live_mount_info (json); JSON text file with the Rubrik cluster response to the live mount request
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
                raise RubrikOracleDBMountError("I/O error({0}): {1}".format(e.errno, e.strerror))
            except Exception:
                raise RubrikOracleDBMountError("Unexpected error: {}".format(sys.exc_info()[0]))
            base64_aco_byte_file = base64.b64encode(aco_file.encode("utf-8"))
            base64_aco_file = str(base64_aco_byte_file, "utf-8")
    if pfile:
        logger.warning("Using custom PFILE File: {}.".format(pfile))
        if aco_parameters:
            logger.debug("ACO Parameters: {0}".format(aco_parameters))
            for config in aco_parameters:
                if config[0].upper() != 'ORACLE_HOME' and config[0].upper() != 'SPFILE_LOCATION' and config[0][:-1].upper() != 'DB_CREATE_ONLINE_LOG_DEST_':
                    raise RubrikOracleDBMountError("When using a custom PFILE the only parameters allowed in the ACO file are ORACLE_HOME, SPFILE_LOCATION and DB_CREATE_ONLINE_LOG_DEST_*.")
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
                raise RubrikOracleDBMountError("When cloning a DG Group database, the ORACLE_HOME must be provided")
        else:
            logger.warning("ORACLE_HOME must be specified for a DG Group.")
            raise RubrikOracleDBMountError("When cloning a DG Group database, the ORACLE_HOME must be provided")
    if oracle_home and database.v6:
        logger.debug("ORACLE_HOME is {0}".format(oracle_home))
    elif oracle_home:
        raise RubrikOracleDBMountError("The Oracle Home parameter is not supported with pre 6.0 CDM.")

    logger.warning("Starting Live Mount of {0} on {1}.".format(source_host_db[1], host_target))
    logger.debug("db_clone parameters host_id={0}, time_ms={1}, pfile={2}, aco_file_path={3}, oracle_home={4}".format(host_id, time_ms, pfile, aco_file_path, oracle_home))
    live_mount_info = database.live_mount(host_id=host_id, time_ms=time_ms, pfile=pfile, aco_file=base64_aco_file, oracle_home=oracle_home)
    logger.debug(live_mount_info)
    # Set the time format for the printed result
    cluster_timezone = pytz.timezone(rubrik.timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(live_mount_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    logger.debug("Live mount status: {0}, Started at {1}.".format(live_mount_info['status'], start_time.strftime(fmt)))
    if no_wait:
        return live_mount_info
    else:
        live_mount_info = database.async_requests_wait(live_mount_info['id'], 12)
        logger.warning("Async request completed with status: {}".format(live_mount_info['status']))
        if live_mount_info['status'] != "SUCCEEDED":
            raise RubrikOracleDBMountError(
                "Mount of Oracle DB did not complete successfully. Mount ended with status {}".format(
                    live_mount_info['status']))
        logger.warning("Live mount of the backup files completed.")
        return live_mount_info


class RubrikOracleDBMountError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
