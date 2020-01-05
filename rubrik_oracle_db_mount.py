import rubrik_oracle_module as rbk
import click
import datetime
import pytz


@click.command()
@click.argument('host_cluster_db')
@click.option('--time_restore', '-t', type=str, help='Point in time to mount the DB, format is YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15')
@click.option('--host', '-h', type=str, help='Host or RAC cluster name (RAC target required if source is RAC)  for the Live Mount ')
@click.option('--files', '-f', is_flag=True, help='Only mount the backup pieces')
@click.option('--path', '-p', type=str, help='Path where the backup pieces will be mounted')
def cli(host_cluster_db, time_restore, host, files, path):
    cluster_info = rbk.get_cluster_info()
    timezone = cluster_info['timezone']['timezone']
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    host_cluster_db = host_cluster_db.split(":")
    oracle_db_id = rbk.get_oracle_db_id(host_cluster_db[1], host_cluster_db[0])
    oracle_db_info = rbk.get_oracle_db_info(oracle_db_id)
    rac = False
    if 'racName' in oracle_db_info.keys():
        if oracle_db_info['racName']:
            rac = True
    if not host:
        print("No live mount host entered, using the source host for a live mount of the requested backup set.")
        host = host_cluster_db[0]
        files = True
        if not path:
            print("The Mount Path must be provided for a files only mount!")
            exit(1)
    host_id = rbk.get_oracle_host_or_rac_id(host, rac)
    if time_restore:
        time_ms = rbk.epoch_time(time_restore, timezone)
        # print("Debug: Epoch Time is {}".format(time_ms))
        print("Using {} for mount.". format(time_restore))
    else:
        print("Using most recent recovery point for mount.")
        oracle_db_info = rbk.get_oracle_db_info(oracle_db_id)
        time_ms = rbk.epoch_time(oracle_db_info['latestRecoveryPoint'], timezone)
        # print("Debug: Using latest recovery point: {}".format(oracle_db_info['latestRecoveryPoint']))
        # print("Debug: Epoch Time is {}".format(time_ms))
    if files:
        if not path:
            print("The Mount Path must be provided for a files only mount!")
            exit(1)

    print("Starting Live Mount of {} on {}.".format(host_cluster_db[0], host))
    live_mount_info = rbk.live_mount(oracle_db_id, host_id, time_ms, files_only=files, mount_path=path)
    cluster_timezone = pytz.timezone(timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(live_mount_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    print("Live mount status: {}, Started at {}.".format(live_mount_info['status'], start_time.strftime(fmt)))


if __name__ == "__main__":
    cli()
