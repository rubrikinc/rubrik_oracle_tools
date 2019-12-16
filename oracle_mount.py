import rubrik_cdm
import click
import json
import datetime
import pytz
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
@click.option('--time_restore', '-t', type=str, help='Point in time to mount the DB, format is YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15')
@click.option('--host', '-h', type=str, help='Host or RAC cluster name (RAC target required if source is RAC)  for the Live Mount ')
@click.option('--files', '-f', is_flag=True, help='Only mount the backup pieces')
@click.option('--path', '-p', type=str, help='Path where the backup pieces will be mounted')
def cli(oracle_db_name, oracle_host_name, time_restore, host, files, path):
    cluster_info = get_cluster_info()
    timezone = cluster_info['timezone']['timezone']
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    oracle_db_id = get_oracle_db_id(oracle_db_name, oracle_host_name)
    oracle_db_info = get_oracle_db_info(oracle_db_id)
    rac = False
    if oracle_db_info['racName']:
        rac = True
    host_id = get_oracle_host_or_rac_id(host, rac)
    if time_restore:
        time_ms = epoch_time(time_restore, timezone)
        # print("Debug: Epoch Time is {}".format(time_ms))
        print("Using {} for mount.". format(time_restore))
    else:
        print("Using most recent recovery point for mount.")
        oracle_db_info = get_oracle_db_info(oracle_db_id)
        time_ms = epoch_time(oracle_db_info['latestRecoveryPoint'], timezone)
        # print("Debug: Using latest recovery point: {}".format(oracle_db_info['latestRecoveryPoint']))
        # print("Debug: Epoch Time is {}".format(time_ms))
    if files:
        if not path:
            print("The Mount Path must be provided for a files only mount!")
            exit(0)

    print("Starting Live Mount of {} on {}.".format(oracle_db_name, host))
    live_mount_info = live_mount(oracle_db_id, host_id, time_ms, files_only=files, mount_path=path)
    cluster_timezone = pytz.timezone(timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(live_mount_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    print("Live mount status: {}, Started at {}.".format(live_mount_info['status'], start_time.strftime(fmt)))


def get_oracle_db_id(oracle_db_name, oracle_host_name):
    oracle_db_id = rubrik.object_id(oracle_db_name, 'oracle_db', hostname=oracle_host_name)
    return oracle_db_id


def epoch_time(time_string, timezone):
    if time_string.endswith('Z'):
        # print("Debug: Using UTC")
        time_string = time_string[:-1]
        utc = pytz.utc
        datetime_object = utc.localize(datetime.datetime.fromisoformat(time_string))
    else:
        # print("Debug: Using time {}.".format(time_string))
        cluster_timezone = pytz.timezone(timezone)
        datetime_object = cluster_timezone.localize(datetime.datetime.fromisoformat(time_string))
    return int(datetime_object.timestamp()) * 1000


def get_oracle_db_info(oracle_db_id):
    oracle_db_info = rubrik.get('internal', '/oracle/db/{}'.format(oracle_db_id))
    return oracle_db_info


def get_cluster_info():
    cluster_info = rubrik.get('v1','/cluster/me')
    return cluster_info


def live_mount(oracle_db_id, host_id, time_ms, files_only=False, mount_path=None):
    payload = {
        "recoveryPoint": {"timestampMs": time_ms},
        "targetOracleHostOrRacId": host_id,
        "targetMountPath": mount_path,
        "shouldMountFilesOnly": files_only
    }
    live_mount_info = rubrik.post('internal', '/oracle/db/{}/mount'.format(oracle_db_id), payload)
    return live_mount_info


def get_oracle_host_or_rac_id(hostname, rac):
    host_id = ''
    if rac:
        host_id = get_oracle_rac_id(hostname)
        if not host_id:
            print("Source database is RAC so restore target must be a RAC cluster.")
            print("The target: {} either was not found or is not a RAC cluster.".format(hostname))
            exit(1)
    else:
        host_info = rubrik.get('internal', '/oracle/host?name={}'.format(hostname))
        if host_info['total'] == 0:
            print("The target: {} either was not found.".format(hostname))
            exit(1)
        elif host_info['total'] > 1:
            print("Multiple Host IDs found:")
            for hosts in host_info['data']:
                print("Host: {}, ID: {}.".format(hosts['name'], hosts['id']))
            exit(1)
        else:
            host_id = host_info['data'][0]['id']
    # return host_info['total'], ids
    return host_id


def get_oracle_rac_id(rac_cluster_name):
    rac_info = rubrik.get('internal', '/oracle/rac?name={}'.format(rac_cluster_name))
    rac_id = ''
    for rac in rac_info['data']:
        if rac_cluster_name == rac['name']:
            # print("Debug: RAC id is {}.".format(rac['id']))
            rac_id = rac['id']
    return rac_id


if __name__ == "__main__":
    cli()
