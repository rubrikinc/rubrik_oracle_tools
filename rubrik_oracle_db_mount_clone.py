import rbs_oracle_common
import click
import logging
import sys
import os
import platform
import datetime
import pytz


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--host_target', '-h', required=True, type=str, help='Host or RAC cluster name (RAC target required if source is RAC)  for the Live Mount ')
@click.option('--new_oracle_name', '-n', required=True, type=str, help='Name for the cloned database')
@click.option('--time_restore', '-t', type=str, help='Point in time to mount the DB, iso format is YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL.')
def cli(source_host_db, host_target, time_restore, new_oracle_name, debug_level):
    """Live mount an Oracle database from a Rubrik Oracle Backup and rename the live mounted database.

\b
    Live mounts an Oracle database from the Rubrik backups. The database is then shutdown, mounted, and
    the name changed using the Oracle NID utility. Note that live mounted databases that have had the
    name changed will need to be cleaned up after the database is unmounted. The
    rubrik_oracle_db_clone_unmount utility will both unmount the live mount and cleanup the database
    files.

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

    # Make sure this is being run on the target host
    if host_target.split('.')[0] != platform.uname()[1].split('.')[0]:
        raise RubrikOracleDBMountCloneError("This program must be run on the target host: {}".format(host_target))
    rubrik = rbs_oracle_common.RubrikConnection()
    source_host_db = source_host_db.split(":")
    database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, source_host_db[1], source_host_db[0])
    oracle_db_info = database.get_oracle_db_info()
    host_id = ''
    # If source DB is RAC then the target for the live mount must be a RAC cluster
    if 'racName' in oracle_db_info.keys():
        if oracle_db_info['racName']:
            host_id = database.get_rac_id(rubrik.cluster_id, host_target)
    else:
        host_id = database.get_host_id(rubrik.cluster_id, host_target)
    if time_restore:
        time_ms = database.epoch_time(time_restore, rubrik.timezone)
        print("Using {} for mount.". format(time_restore))
    else:
        print("Using most recent recovery point for mount.")
        time_ms = database.epoch_time(oracle_db_info['latestRecoveryPoint'], rubrik.timezone)
    print("Starting Live Mount of {} on {}.".format(source_host_db[1], host_target))
    live_mount_info = database.live_mount(host_id, time_ms)
    # Set the time format for the printed result
    cluster_timezone = pytz.timezone(rubrik.timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(live_mount_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    print("Live mount requested at {}.".format(start_time.strftime(fmt)))
    live_mount_info = database.async_requests_wait(live_mount_info['id'], 20)
    print("Async request completed with status: {}".format(live_mount_info['status']))
    if live_mount_info['status'] != "SUCCEEDED":
        raise RubrikOracleDBMountCloneError(
            "Mount of backup files did not complete successfully. Mount ended with status {}".format(
                live_mount_info['status']))
    print("Live mount of the databases completed. Changing name...")
    oracle_home = oracle_db_info['oracleHome']
    if not os.path.exists(oracle_home):
        raise RubrikOracleDBMountCloneError("The ORACLE_HOME: {} does not exist on the target host: {}".format(oracle_home, host_target))
    database.oracle_db_rename(source_host_db[1], oracle_home, new_oracle_name)
    print("DB Live Mount with  name {} complete.".format(new_oracle_name))
    rubrik.delete_session()
    return


class RubrikOracleDBMountCloneError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()