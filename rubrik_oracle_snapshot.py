import rbs_oracle_common
import click
import logging
import sys
import datetime
import pytz


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--force', '-f', is_flag=True, help='Force a new full database image level 0 backup')
@click.option('--sla', type=str, help='Rubrik SLA Domain to use if different than the assigned SLA')
@click.option('--wait', is_flag=True, help='Wait for backup to complete.')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL.')
def cli(source_host_db, force, sla, wait, debug_level):
    """
    This will initiate an on demand snapshot (backup) of the database.

\b
    The source database is specified in a host:db format. To force a new full level 0
    image backup of the database set force to True. If you would like to use a different SLA for this snapshot you
    can specify that here also. Note if no SLA is supplied the current sla for this database will be used.

\b
    Returns:
        snapshot_info (dict): The information about the snapshot returned from the Rubrik CDM.
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
    database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, source_host_db[1], source_host_db[0])
    if sla:
        oracle_db_sla_id = database.get_sla_id(sla)
    else:
        oracle_db_info = database.get_oracle_db_info()
        oracle_db_sla_id = oracle_db_info['effectiveSlaDomainId']
    oracle_snapshot_info = database.oracle_db_snapshot(oracle_db_sla_id, force)
    logging.debug(oracle_snapshot_info)
    if wait:
        logger.warning("Starting backup (snapshot) of database {} on {}".format(source_host_db[1], source_host_db[0]))
        oracle_snapshot_info = database.async_requests_wait(oracle_snapshot_info['id'], 12)
        logger.warning("Async request completed with status: {}".format(oracle_snapshot_info['status']))
        if oracle_snapshot_info['status'] != "SUCCEEDED":
            raise RubrikOracleSnapshotError(
                "Database backup (snapshot) did not complete successfully. Mount ended with status {}".format(
                    oracle_snapshot_info['status']))
        logger.warning("Database backup (snapshot) completed.")
    else:
        cluster_timezone = pytz.timezone(rubrik.timezone)
        utc = pytz.utc
        start_time = utc.localize(datetime.datetime.fromisoformat(oracle_snapshot_info['startTime'][:-1])).astimezone(
            cluster_timezone)
        fmt = '%Y-%m-%d %H:%M:%S %Z'
        logging.warning("Oracle Database snapshot {} \nStatus: {}, Started at {}.".format(oracle_snapshot_info['id'], oracle_snapshot_info['status'], start_time.strftime(fmt)))
    rubrik.delete_session()
    return oracle_snapshot_info


class RubrikOracleSnapshotError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
