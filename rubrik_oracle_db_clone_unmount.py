import click
import rubrik_oracle_module as rbk
import pytz
import datetime


@click.command()
@click.argument('host_cluster_db')
@click.argument('new_oracle_name')
@click.argument('oracle_home')
def cli(host_cluster_db, new_oracle_name, oracle_home):
    """
    This will unmount a Rubrik live mount that has had the name changed after the live mount
     using the the live mount host:Original DB Name, new Oracle DB name and the ORACLE_HOME

\b
    Args:
        host_cluster_db (str): The hostname the database is running on : The source database name
        new_oracle_name (str): The name the source db was changed to.
        oracle_home (str): The ORACLE_HOME on the live mount host.
\b
    Returns:
        unmount_info (dict): Status of the unmount request.
    """
    rubrik = rbk.connect_rubrik()
    cluster_info = rbk.get_cluster_info(rubrik)
    timezone = cluster_info['timezone']['timezone']
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    host_cluster_db = host_cluster_db.split(":")
    live_mount_ids = rbk.get_oracle_live_mount_id(rubrik, cluster_info['id'], host_cluster_db[1], host_cluster_db[0])
    if not live_mount_ids:
        raise RubrikOracleUnmountError("No live mounts found for {} live mounted on {}. ".format(host_cluster_db[1], host_cluster_db[0]))
    else:
        # Check for 0, 1 and >1 returns error on anything but 1
        if len(live_mount_ids) == 0:
            raise RubrikOracleUnmountError(
                "No live mounts found for {} live mounted on {}. ".format(host_cluster_db[1], host_cluster_db[0]))
        elif len(live_mount_ids) > 1:
            raise RubrikOracleUnmountError(
                "More than one backup of {} is live mounted on {}. ".format(host_cluster_db[1], host_cluster_db[0]))
        else:
            force = True
            unmount_info = rbk.live_mount_delete(rubrik, live_mount_ids[0], force)
            # Set the time format for the printed result
            cluster_timezone = pytz.timezone(timezone)
            utc = pytz.utc
            start_time = utc.localize(datetime.datetime.fromisoformat(unmount_info['startTime'][:-1])).astimezone(
                cluster_timezone)
            fmt = '%Y-%m-%d %H:%M:%S %Z'
            print("Rubrik Unmount status: {}, Started at {}.".format(unmount_info['status'], start_time.strftime(fmt)))
            unmount_info = rbk.request_status_wait_loop(rubrik, unmount_info['id'], 'QUEUED', 3)
            unmount_info = rbk.request_status_wait_loop(rubrik, unmount_info['id'], 'RUNNING', 10)
            unmount_info = rbk.request_status_wait_loop(rubrik, unmount_info['id'], 'FINISHING', 5)
            if rubrik.get('internal', '/oracle/request/{}'.format(unmount_info['id']), timeout=60)[
                'status'] != "SUCCEEDED":
                return unmount_info
            rbk.oracle_db_clone_cleanup(new_oracle_name, oracle_home)
            return unmount_info


class RubrikOracleUnmountError(rbk.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
