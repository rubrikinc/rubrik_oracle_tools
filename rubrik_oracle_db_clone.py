import rbs_oracle_common
import click
import logging
import sys
import datetime
import pytz


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--host_target', '-h', type=str, required=True, help='Host or RAC cluster name (RAC target required if source is RAC)  for the Live Mount ')
@click.option('--time_restore', '-t', type=str, help='Point in time to mount the DB, iso format is YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15')
@click.option('--new_name', '-n', type=str, help='Name for cloned database')
@click.option('--pfile', '-p', type=str, help='Custom Pfile path (on target host)')
@click.option('--aco_file_path', '-a', type=str, help='ACO file path for parameter changes')
@click.option('--no_wait', is_flag=True, help='Queue Live Mount and exit.')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL.')
def cli(source_host_db, host_target, time_restore, new_name, pfile, aco_file_path, no_wait, debug_level):
    """Clones an Oracle Database (alternate host restore or duplicate).

    Initiates an Oracle DB clone using the Rubrik RBS automated clone.

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
    aco_file = None
    if aco_file_path:
        logger.warning("Using ACO File: {}".format(aco_file_path))
        try:
            aco_file = open(aco_file_path, "r").read()
        except IOError as e:
            raise RubrikOracleDBCloneError("I/O error({0}): {1}".format(e.errno, e.strerror))
        except:
            raise RubrikOracleDBCloneError("Unexpected error: {}".format(sys.exc_info()[0]))
    if pfile:
        logger.warning("Using custom pfile File: {}.".format(pfile))
    if new_name:
        if not aco_file and not pfile:
            logger.warning("Using a new database name requires either an ACO file or a custom pfile")
            logger.warning("The following parameters are required: db_file_name_convert, log_file_name_convert, parameter_value_convert or control_files, db_create_file_dest")
        else:
            logger.warning("Starting Clone of {0} to {1} on {2}".format(database.database_name, new_name, host_target))
    else:
        logger.warning("Starting Clone of {0} on {1}.".format(source_host_db[1], host_target))
    db_clone_info = database.db_clone(host_id, time_ms, False, None, new_name, pfile, aco_file)
    logger.debug(db_clone_info)
    # Set the time format for the printed result
    cluster_timezone = pytz.timezone(rubrik.timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(db_clone_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    logger.debug("Live mount status: {0}, Started at {1}.".format(db_clone_info['status'], start_time.strftime(fmt)))
    if no_wait:
        return db_clone_info
    else:
        db_clone_info = database.async_requests_wait(db_clone_info['id'], 12)
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
