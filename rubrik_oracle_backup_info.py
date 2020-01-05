
import click
import json
import rubrik_oracle_module as rbk


@click.command()
@click.argument('host_cluster_db')
def cli(host_cluster_db):
    cluster_info = rbk.get_cluster_info()
    timezone = cluster_info['timezone']['timezone']
    print("")
    print("*" * 100)
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    host_cluster_db = host_cluster_db.split(":")
    oracle_db_id = rbk.get_oracle_db_id(host_cluster_db[1], host_cluster_db[0])
    oracle_db_info = rbk.get_oracle_db_info(oracle_db_id)
    # print_json(oracle_db_info)
    print("*" * 100)
    print("Database Details: ")
    print("Database name: {}   ID: {}".format(oracle_db_info['name'], oracle_db_info['id']))
    if 'standaloneHostName' in oracle_db_info.keys():
        print("Host Name: {}".format(oracle_db_info['standaloneHostName']))
    elif 'racName' in oracle_db_info.keys():
        print("Rac Cluster Name: {}    Instances: {}".format(oracle_db_info['racName'], oracle_db_info['numInstances']))
    print("SLA: {}    Log Backup Frequency: {} min.    Log Retention: {} hrs.".format(oracle_db_info['effectiveSlaDomainName'], oracle_db_info['logBackupFrequencyInMinutes'], oracle_db_info['logRetentionHours']))

    oracle_snapshot_info = rbk.get_oracle_db_snapshots(oracle_db_id)
    print_oracle_db_snapshots(oracle_snapshot_info, timezone)
    oracle_db_recoverable_range_info = rbk.get_oracle_db_recoverable_range(oracle_db_id)
    print_oracle_db_recoverable_range(oracle_db_recoverable_range_info, timezone)


def print_json(info_dictionary):
    print(json.dumps(info_dictionary, indent=2))


def print_oracle_db_snapshots(oracle_db_snapshot_info, timezone):
    print("*" * 100)
    print("Available Database Backups (Snapshots):")
    for snap in oracle_db_snapshot_info['data']:
        print("Database Backup Date: {}   Snapshot ID: {}".format(rbk.cluster_time(snap['date'], timezone), snap['id']))


def print_oracle_db_recoverable_range(oracle_db_recoverable_range_info, timezone):
    print("*" * 100)
    print("Recoverable ranges:")
    for recovery_range in oracle_db_recoverable_range_info['data']:
        print("Begin Time: {}   End Time: {}".format(rbk.cluster_time(recovery_range['beginTime'], timezone), rbk.cluster_time(recovery_range['endTime'], timezone)))


if __name__ == "__main__":
    cli()
