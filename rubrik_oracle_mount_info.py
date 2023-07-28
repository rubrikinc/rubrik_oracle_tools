import rbs_oracle_common
import click
import logging
import sys


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--mounted_host', '-m', type=str, required=True,  help='The host with the live mount to remove')
@click.option('--keyfile', '-k', type=str, required=False,  help='The connection keyfile path')
@click.option('--insecure', is_flag=True,  help='Flag to use insecure connection')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(source_host_db, mounted_host, keyfile, insecure, debug_level):
    """
    This will print the information about a Rubrik live mount using the database name and the live mount host.

\b
    Returns:
        
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

    rubrik = rbs_oracle_common.RubrikConnection(keyfile, insecure)
    source_host_db = source_host_db.split(":")
    mount = rbs_oracle_common.RubrikRbsOracleMount(rubrik, source_host_db[1], source_host_db[0], mounted_host)
    live_mount_ids = mount.get_oracle_live_mount_id()
    if not live_mount_ids:
        raise RubrikOracleMountInfoError("No live mounts found for {} live mounted on {}. ".format(source_host_db[1], mounted_host))
    else:
        print("Live mounts of {} mounted on {}:".format(source_host_db[1], mounted_host))
        for live_mount_id in live_mount_ids:
            logger.info("Getting info for mount with id: {}.".format(live_mount_id))
            mount_information = mount.get_live_mount_info(live_mount_id)
            logger.debug("mount_info: {0}".format(mount_information))
            print("Source DB: {}  Source Host: {}  Mounted Host: {}  Owner: {}  Created: {}  Status: {}  id: {}".format(
                source_host_db[1], source_host_db[0], mounted_host, mount_information.get('ownerName', 'None'),
                mount_information['creationDate'], mount_information['status'], mount_information['id']))
        rubrik.delete_session()
        return


class RubrikOracleMountInfoError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
