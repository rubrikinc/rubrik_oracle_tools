import click
import rubrik_oracle_module as rbk


@click.command()
@click.argument('host_cluster_db')
@click.option('--force', '-f', is_flag=True, help='Force unmount')
def cli(host_cluster_db, force):
    """
    This will unmount a Rubrik live mount using the database name and the live mount host.

    Args:
        host_cluster_db (str): The hostname the database is running on : The database name
        force (bool): Force the unmount

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
        for live_mount_id in live_mount_ids:
            unmount_info = rbk.live_mount_delete(rubrik, live_mount_id, force)
            print("Live mount id: {} Unmount status: {}.".format(live_mount_id, unmount_info['status']))
            return unmount_info


class RubrikOracleUnmountError(rbk.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
