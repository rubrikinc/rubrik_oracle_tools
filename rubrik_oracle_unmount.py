import click
import rubrik_oracle_module as rbk


@click.command()
@click.argument('host_cluster_db')
@click.option('--force', '-f', is_flag=True, help='Force unmount')
def cli(host_cluster_db, force):
    cluster_info = rbk.get_cluster_info()
    timezone = cluster_info['timezone']['timezone']
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    host_cluster_db = host_cluster_db.split(":")
    live_mount_ids = rbk.get_oracle_live_mount_id(host_cluster_db[1], host_cluster_db[0])
    if not live_mount_ids:
        print("No live mounts found for {} live mounted on {}. ".format(host_cluster_db[1], host_cluster_db[0]))
    else:
        for live_mount_id in live_mount_ids:
            unmount_info = rbk.live_mount_delete(live_mount_id, force)
            print("Live mount id: {} Unmount status: {}.".format(live_mount_id, unmount_info['status']))


if __name__ == "__main__":
    cli()
