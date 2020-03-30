import rubrik_oracle_module as rbk
import click
import datetime
import pytz


@click.command()
@click.argument('host_cluster_db')
@click.argument('target_host')
@click.option('--time_restore', '-t', type=str, help='Point in time to mount the DB, iso format is YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15')
@click.option('--oracle_home', '-o', type=str, help='ORACLE_HOME path for this database')
@click.option('--new_oracle_name', '-n', type=str, help='Name for the cloned database')
def cli(host_cluster_db, target_host, time_restore, oracle_home, new_oracle_name):
    """Live mount an Oracle database from a Rubrik Oracle Backup and rename the live mounted database.

\b
    Live mounts an Oracle database from the Rubrik backups. The database is then shutdown, mounted, and
    the name changed using the Oracle NID utility. Note that live mounted databases that have had the
    name changed will need to be cleaned up after the database is unmounted. The
    rubrik_oracle_db_clone_unmoount utility will both unmount the live mount and cleanup the database
    files.

\b
    Args:
        host_cluster_db (str): The hostname the database is running on : The database name
        target_host (str): The host to live mount the database. (Must be a compatible Oracle host on Rubrik)
        time_restore: The point in time for the live mount iso 8601 format (2019-04-30T18:23:21)
        oracle_home (str): The ORACLE_HOME on the host where there live mount is being done.
        new_oracle_name (str): The new name for the live mounted database.
\b
    Returns:
        live_mount_info (json); JSON text file with the Rubrik cluster response to the live mount request
    """
    rubrik = rbk.connect_rubrik()
    cluster_info = rbk.get_cluster_info(rubrik)
    timezone = cluster_info['timezone']['timezone']
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    host_cluster_db = host_cluster_db.split(":")
    oracle_db_id = rbk.get_oracle_db_id(rubrik, host_cluster_db[1], host_cluster_db[0])
    oracle_db_info = rbk.get_oracle_db_info(rubrik, oracle_db_id)
    host_id = ''
    # If source DB is RAC then the target for the live mount must be a RAC cluster
    if 'racName' in oracle_db_info.keys():
        if oracle_db_info['racName']:
            host_id = rbk.get_rac_id(rubrik, cluster_info['id'], target_host)
    else:
        host_id = rbk.get_host_id(rubrik, cluster_info['id'], target_host)
    if time_restore:
        time_ms = rbk.epoch_time(time_restore, timezone)
        print("Using {} for mount.". format(time_restore))
    else:
        print("Using most recent recovery point for mount.")
        oracle_db_info = rbk.get_oracle_db_info(rubrik, oracle_db_id)
        time_ms = rbk.epoch_time(oracle_db_info['latestRecoveryPoint'], timezone)
    print("Starting Live Mount of {} on {}.".format(host_cluster_db[1], target_host))
    live_mount_info = rbk.live_mount(rubrik, oracle_db_id, host_id, time_ms)
    # Set the time format for the printed result
    cluster_timezone = pytz.timezone(timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(live_mount_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    print("Live mount requested at {}.".format(start_time.strftime(fmt)))
    live_mount_info = rbk.async_requests_wait(rubrik, live_mount_info['id'], 20)
    print("Async request completed with status: {}".format(live_mount_info['status']))
    if live_mount_info['status'] != "SUCCEEDED":
        raise RubrikOracleDBMountCloneError(
            "Mount of backup files did not complete successfully. Mount ended with status {}".format(
                live_mount_info['status']))
    print("Live mount of the databases completed. Changing name...")

    rbk.oracle_db_rename(host_cluster_db[1], oracle_home, new_oracle_name)
    print("DB Live Mount with new name {} complete.".format(new_oracle_name))
    return


class RubrikOracleDBMountCloneError(rbk.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()