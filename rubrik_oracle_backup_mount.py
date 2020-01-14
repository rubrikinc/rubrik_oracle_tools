
import rubrik_oracle_module as rbk
import click
import datetime
import pytz


@click.command()
@click.argument('host_cluster_db')
@click.argument('path')
@click.option('--time_restore', '-t', type=str, help='Point in time to mount the DB, format is YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15')
@click.option('--host', '-h', type=str, help='Host or RAC cluster name (RAC target required if source is RAC)  for the Live Mount ')
def cli(host_cluster_db, path, time_restore, host):
    """
    This will mount the requested Rubrik Oracle backup set on the provided path.

    The source database is specified in a host:db format. The mount path is required. If the restore time is not
    provided the most recent recoverable time will be used. The host for the mount can be specified if it is not it
    will be mounted on the source host.

    Args:
        host_cluster_db (str): The hostname the database is running on : The database name.
        path (str): The path for the mount. This must exist on the requested host.
        time_restore (str): The point in time for the backup set in  iso 8601 format (2019-04-30T18:23:21).
        host (str): The host to mount the backup set. If not specified the source host will be used.

    Returns:
        live_mount_info (dict): The information about the requested files only mount returned from the Rubrik CDM.
    """
    rubrik = rbk.connect_rubrik()
    cluster_info = rbk.get_cluster_info(rubrik)
    timezone = cluster_info['timezone']['timezone']
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    host_cluster_db = host_cluster_db.split(":")
    oracle_db_id = rbk.get_oracle_db_id(rubrik, host_cluster_db[1], host_cluster_db[0])
    oracle_db_info = rbk.get_oracle_db_info(rubrik, oracle_db_id)
    if 'racName' in oracle_db_info.keys():
        if oracle_db_info['racName']:
            host_id = rbk.get_rac_id(rubrik, host)
    else:
        host_id = rbk.get_host_id(rubrik, host)
    if time_restore:
        time_ms = rbk.epoch_time(time_restore, timezone)
        print("Using {} for mount.". format(time_restore))
    else:
        print("Using most recent recovery point for mount.")
        oracle_db_info = rbk.get_oracle_db_info(rubrik, oracle_db_id)
        time_ms = rbk.epoch_time(oracle_db_info['latestRecoveryPoint'], timezone)
    print("Starting the mount of the requested {} backup pieces on {}.".format(host_cluster_db[1], host))
    live_mount_info = rbk.live_mount(rubrik, oracle_db_id, host_id, time_ms, files_only=True, mount_path=path)
    cluster_timezone = pytz.timezone(timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(live_mount_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    print("Live mount status: {}, Started at {}.".format(live_mount_info['status'], start_time.strftime(fmt)))
    return live_mount_info


if __name__ == "__main__":
    cli()
