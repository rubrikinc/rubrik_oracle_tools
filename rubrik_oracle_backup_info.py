
import click
import rubrik_oracle_module as rbk


@click.command()
@click.argument('host_cluster_db')
def cli(host_cluster_db):
    """
    Displays information about the Oracle database object, the available snapshots, and recovery ranges.

    Args:
        host_cluster_db (str): The hostname the database is running on : The database name

    Returns:
        None: Information is printed to standard out
    """
    rubrik = rbk.connect_rubrik()
    cluster_info = rbk.get_cluster_info(rubrik)
    timezone = cluster_info['timezone']['timezone']
    print("")
    print("*" * 100)
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    host_cluster_db = host_cluster_db.split(":")
    oracle_db_id = rbk.get_oracle_db_id(rubrik, host_cluster_db[1], host_cluster_db[0])
    oracle_db_info = rbk.get_oracle_db_info(rubrik, oracle_db_id)
    print("*" * 100)
    print("Database Details: ")
    print("Database name: {}   ID: {}".format(oracle_db_info['name'], oracle_db_info['id']))
    if 'standaloneHostName' in oracle_db_info.keys():
        print("Host Name: {}".format(oracle_db_info['standaloneHostName']))
    elif 'racName' in oracle_db_info.keys():
        print("Rac Cluster Name: {}    Instances: {}".format(oracle_db_info['racName'], oracle_db_info['numInstances']))
    print("SLA: {}    Log Backup Frequency: {} min.    Log Retention: {} hrs.".format(oracle_db_info['effectiveSlaDomainName'], oracle_db_info['logBackupFrequencyInMinutes'], oracle_db_info['logRetentionHours']))
    oracle_snapshot_info = rbk.get_oracle_db_snapshots(rubrik, oracle_db_id)
    print("*" * 100)
    print("Available Database Backups (Snapshots):")
    for snap in oracle_snapshot_info['data']:
        print("Database Backup Date: {}   Snapshot ID: {}".format(rbk.cluster_time(snap['date'], timezone), snap['id']))
    oracle_db_recoverable_range_info = rbk.get_oracle_db_recoverable_range(rubrik,  oracle_db_id)
    print("*" * 100)
    print("Recoverable ranges:")
    for recovery_range in oracle_db_recoverable_range_info['data']:
        print("Begin Time: {}   End Time: {}".format(rbk.cluster_time(recovery_range['beginTime'], timezone),
                                                     rbk.cluster_time(recovery_range['endTime'], timezone)))


if __name__ == "__main__":
    cli()
