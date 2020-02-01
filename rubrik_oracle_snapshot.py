import rubrik_oracle_module as rbk
import click
import pytz
import datetime


@click.command()
@click.argument('host_cluster_db')
@click.option('--force', '-f', is_flag=True, help='Force a new full database image level 0 backup')
@click.option('--sla', '-s', type=str, help='Rubrik SLA Domain to use if different than the assigned SLA')
def cli(host_cluster_db, force, sla):
    """
    This will initiate an on demand snapshot (backup) of the database.

\b
    The source database is specified in a host:db format. To force a new full level 0
    image backup of the database set force to True. If you would like to use a different SLA for this snapshot you
    can specify that here also. Note if no SLA is supplied the current sla for this database will be used.
\b
    Args:
        host_cluster_db (str): The hostname the database is running on : The database name.
        force (bool): Force a new full database image level 0 backup
        sla (str): The Rubrik SLA Domain to use if different than the assigned SLA
\b
    Returns:
        snapshot_info (dict): The information about the snapshot returned from the Rubrik CDM.
    """
    rubrik = rbk.connect_rubrik()
    cluster_info = rbk.get_cluster_info(rubrik)
    timezone = cluster_info['timezone']['timezone']
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    host_cluster_db = host_cluster_db.split(":")
    oracle_db_id = rbk.get_oracle_db_id(rubrik, host_cluster_db[1], host_cluster_db[0])
    if sla:
        oracle_db_sla_id = rbk.get_sla_id(rubrik, sla)
    else:
        oracle_db_info = rbk.get_oracle_db_info(rubrik, oracle_db_id)
        oracle_db_sla_id = oracle_db_info['effectiveSlaDomainId']
    oracle_snapshot_info = rbk.oracle_db_snapshot(rubrik, oracle_db_id, oracle_db_sla_id, force)
    cluster_timezone = pytz.timezone(timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(oracle_snapshot_info['startTime'][:-1])).astimezone(
        cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    print("Oracle Database snapshot {} \nStatus: {}, Started at {}.".format(oracle_snapshot_info['id'], oracle_snapshot_info['status'], start_time.strftime(fmt)))
    return oracle_snapshot_info


if __name__ == "__main__":
    cli()