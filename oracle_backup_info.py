import rubrik_cdm
import click
import json
import os
import urllib3
urllib3.disable_warnings()

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
with open(os.path.join(__location__, 'config.json')) as config_file:
    config = json.load(config_file)
if not config['rubrik_cdm_node_ip']:
    config['rubrik_cdm_node_ip'] = None
rubrik = rubrik_cdm.Connect(config['rubrik_cdm_node_ip'], config['rubrik_cdm_username'], config['rubrik_cdm_password'], config['rubrik_cdm_token'])


@click.command()
@click.argument('oracle_db_name')
@click.argument('oracle_host_name')
def cli(oracle_db_name, oracle_host_name):
    oracle_db_id = get_oracle_db_id(oracle_db_name, oracle_host_name)
    oracle_db_info = get_oracle_db_info(oracle_db_id)
    print_json(oracle_db_info)
    oracle_snapshot_info = get_oracle_db_snapshots(oracle_db_id)
    print_oracle_db_snapshots(oracle_snapshot_info)
    oracle_db_recoverable_range_info = get_oracle_db_recoverable_range(oracle_db_id)
    print_oracle_db_recoverable_range(oracle_db_recoverable_range_info)


def get_oracle_db_id(oracle_db_name, oracle_host_name):
    oracle_db_id = rubrik.object_id(oracle_db_name, 'oracle_db', hostname=oracle_host_name)
    return oracle_db_id


def get_oracle_db_info(oracle_db_id):
    oracle_db_info = rubrik.get('internal', '/oracle/db/{}'.format(oracle_db_id))
    return oracle_db_info


def print_json(info_dictionary):
    print(json.dumps(info_dictionary, indent=2))


def get_oracle_db_snapshots(oracle_db_id):
    oracle_db_snapshot_info = rubrik.get('internal', '/oracle/db/{}/snapshot'.format(oracle_db_id))
    return oracle_db_snapshot_info


def print_oracle_db_snapshots(oracle_db_snapshot_info):
    print("Available Database Backups (Snapshots):")
    for snap in oracle_db_snapshot_info['data']:
        print("Database Backup Date: {}   Snapshot ID: {}".format(snap['date'], snap['id']))


def get_oracle_db_recoverable_range(oracle_db_id):
    get_oracle_db_recoverable_range_info = rubrik.get('internal', '/oracle/db/{}/recoverable_range'.format(oracle_db_id))
    return get_oracle_db_recoverable_range_info


def print_oracle_db_recoverable_range(oracle_db_recoverable_range_info):
    print("Recoverable ranges:")
    for recovery_range in oracle_db_recoverable_range_info['data']:
        print("Begin Time: {}   End Time: {}".format(recovery_range['beginTime'], recovery_range['endTime']))


if __name__ == "__main__":
    cli()
