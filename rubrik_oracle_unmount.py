import rbs_oracle_common
import click
import logging
import sys


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--mounted_host', '-m', type=str, required=True,  help='The host with the live mount to remove')
@click.option('--force', '-f', is_flag=True, help='Force unmount')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(source_host_db, mounted_host, force, debug_level):
    """
    This will unmount a Rubrik live mount using the database name and the live mount host.

\b
    Returns:
        unmount_info (dict): Status of the unmount request.
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
    mount = rbs_oracle_common.RubrikRbsOracleMount(rubrik, source_host_db[1], source_host_db[0], mounted_host)
    live_mount_ids = mount.get_oracle_live_mount_id()
    if not live_mount_ids:
        raise RubrikOracleUnmountError("No live mounts found for {} live mounted on {}. ".format(source_host_db[1], mounted_host))
    else:
        unmount_info = []
        for live_mount_id in live_mount_ids:
            unmount_response = mount.live_mount_delete(live_mount_id, force)
            logger.warning("Live mount id: {} Unmount status: {}.".format(live_mount_id, unmount_response['status']))
            unmount_info.append(unmount_response)
        return unmount_info


class RubrikOracleUnmountError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
