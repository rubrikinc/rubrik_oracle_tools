import click
import rubrik_oracle_module as rbk
import pytz
import datetime


@click.command()
@click.argument('host_cluster_db')
@click.option('--new_oracle_name', '-n', type=str, help='Oracle database clone name. If unmounting more than one separate with commas.')
@click.option('--oracle_home', '-o', type=str, help='ORACLE_HOME path for the database(s)')
@click.option('--all', '-a', is_flag=True, help='Unmount all mounts from the source host:db. Provide all the clone names separated by commas.')
def cli(host_cluster_db, new_oracle_name, oracle_home, all):
    """
    This will unmount a Rubrik live mount that has had the name changed after the live mount
     using the the live mount host:Original DB Name, new Oracle DB name and the ORACLE_HOME

\b
    Args:
        host_cluster_db (str): The hostname the database is running on : The source database name
        new_oracle_name (str): The name the source db was changed to.
        oracle_home (str): The ORACLE_HOME on the live mount host.
        all (bool): Unmount all mounts from the source host:db. Provide all the clone names separated by commas.
\b
    Returns:

    """
    rubrik = rbk.connect_rubrik()
    cluster_info = rbk.get_cluster_info(rubrik)
    timezone = cluster_info['timezone']['timezone']
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    host_cluster_db = host_cluster_db.split(":")
    live_mount_ids = rbk.get_oracle_live_mount_id(rubrik, cluster_info['id'], host_cluster_db[1], host_cluster_db[0])
    new_oracle_name = new_oracle_name.split(',')
    force = True
    cluster_timezone = pytz.timezone(timezone)
    utc = pytz.utc
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    if not live_mount_ids:
        raise RubrikOracleUnmountError("No live mounts found for {} live mounted on {}. ".format(host_cluster_db[1], host_cluster_db[0]))
    else:
        if len(live_mount_ids) == 0:
            raise RubrikOracleUnmountError(
                "No live mounts found for {} live mounted on {}. ".format(host_cluster_db[1], host_cluster_db[0]))
        elif len(live_mount_ids) > 1:
            if all:
                for live_mount_id in live_mount_ids:
                    unmount_info = rbk.live_mount_delete(rubrik, live_mount_id, force)
                    start_time = utc.localize(datetime.datetime.fromisoformat(unmount_info['startTime'][:-1])).astimezone(cluster_timezone)
                    print("Rubrik Unmount status: {}, Started at {}.".format(unmount_info['status'], start_time.strftime(fmt)))
                    unmount_info = rbk.request_status_wait_loop(rubrik, unmount_info['id'], 'QUEUED', 3)
                    unmount_info = rbk.request_status_wait_loop(rubrik, unmount_info['id'], 'RUNNING', 10)
                    unmount_info = rbk.request_status_wait_loop(rubrik, unmount_info['id'], 'FINISHING', 5)
                    unmount_status = rubrik.get('internal', '/oracle/request/{}'.format(unmount_info['id']), timeout=60)['status']
                    if unmount_status != "SUCCEEDED":
                        print("Unmount of backup files failed with status: {}")
                for name in new_oracle_name:
                    rbk.oracle_db_clone_cleanup(name, oracle_home)
                    print("Clone database {} has been cleaned.".format(name))
                return
            else:
                raise RubrikOracleUnmountError(
                    "More than one backup of {} is live mounted on {}. Use the --all flag to unmount them all.".format(host_cluster_db[1], host_cluster_db[0]))
        else:
            unmount_info = rbk.live_mount_delete(rubrik, live_mount_ids[0], force)

            start_time = utc.localize(datetime.datetime.fromisoformat(unmount_info['startTime'][:-1])).astimezone(
                cluster_timezone)
            print("Rubrik Unmount status: {}, Started at {}.".format(unmount_info['status'], start_time.strftime(fmt)))
            unmount_info = rbk.request_status_wait_loop(rubrik, unmount_info['id'], 'QUEUED', 3)
            unmount_info = rbk.request_status_wait_loop(rubrik, unmount_info['id'], 'RUNNING', 10)
            unmount_info = rbk.request_status_wait_loop(rubrik, unmount_info['id'], 'FINISHING', 5)
            if rubrik.get('internal', '/oracle/request/{}'.format(unmount_info['id']), timeout=60)[
                'status'] != "SUCCEEDED":
                return unmount_info
            rbk.oracle_db_clone_cleanup(new_oracle_name[0], oracle_home)
            return


class RubrikOracleUnmountError(rbk.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
