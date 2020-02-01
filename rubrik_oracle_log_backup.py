import rubrik_oracle_module as rbk
import click
import pytz
import datetime


@click.command()
@click.argument('host_cluster_db')
def cli(host_cluster_db):
    """
    This will initiate an on demand archive log backup of the database.

\b
    The source database is specified in a host:db format.
\b
    Args:
        host_cluster_db (str): The hostname the database is running on : The database name.
\b
    Returns:
        log_backup_info (dict): The information about the snapshot returned from the Rubrik CDM.
    """
    rubrik = rbk.connect_rubrik()
    cluster_info = rbk.get_cluster_info(rubrik)
    timezone = cluster_info['timezone']['timezone']
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    host_cluster_db = host_cluster_db.split(":")
    oracle_db_id = rbk.get_oracle_db_id(rubrik, host_cluster_db[1], host_cluster_db[0])
    oracle_log_backup_info = rbk.oracle_log_backup(rubrik, oracle_db_id)
    cluster_timezone = pytz.timezone(timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(oracle_log_backup_info['startTime'][:-1])).astimezone(
        cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    print("Oracle Log Backup {} \nStatus: {}, Started at {}.".format(oracle_log_backup_info['id'], oracle_log_backup_info['status'], start_time.strftime(fmt)))
    return oracle_log_backup_info


if __name__ == "__main__":
    cli()