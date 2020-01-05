# Rubrik Oracle Functions
# Functions
import rubrik_cdm
import datetime
import pytz
import json
import os
import urllib3
urllib3.disable_warnings()

# Set up the cluster connection
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
with open(os.path.join(__location__, 'config.json')) as config_file:
    config = json.load(config_file)
if not config['rubrik_cdm_node_ip']:
    config['rubrik_cdm_node_ip'] = None
rubrik = rubrik_cdm.Connect(config['rubrik_cdm_node_ip'], config['rubrik_cdm_username'], config['rubrik_cdm_password'], config['rubrik_cdm_token'])


def get_oracle_db_id(oracle_db_name, oracle_host_name):
    oracle_db_id = rubrik.object_id(oracle_db_name, 'oracle_db', hostname=oracle_host_name)
    return oracle_db_id


def get_oracle_db_info(oracle_db_id):
    oracle_db_info = rubrik.get('internal', '/oracle/db/{}'.format(oracle_db_id))
    return oracle_db_info


def get_oracle_db_recoverable_range(oracle_db_id):
    get_oracle_db_recoverable_range_info = rubrik.get('internal', '/oracle/db/{}/recoverable_range'.format(oracle_db_id))
    return get_oracle_db_recoverable_range_info


def get_oracle_db_snapshots(oracle_db_id):
    oracle_db_snapshot_info = rubrik.get('internal', '/oracle/db/{}/snapshot'.format(oracle_db_id))
    return oracle_db_snapshot_info


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


def cluster_time(time_string, timezone):
    cluster_timezone = pytz.timezone(timezone)
    utc = pytz.utc
    if time_string.endswith('Z'):
        time_string = time_string[:-1]
        datetime_object = utc.localize(datetime.datetime.fromisoformat(time_string))
    else:
        datetime_object = cluster_timezone.localize(datetime.datetime.fromisoformat(time_string))
    cluster_time_object = cluster_timezone.normalize(datetime_object.astimezone(cluster_timezone))
    return cluster_time_object.isoformat()


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


def live_mount_delete(live_mount_id, force):
    live_mount_delete_info = rubrik.delete('internal', '/oracle/db/mount/{}?force={}'.format(live_mount_id, force))
    return live_mount_delete_info


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


def get_oracle_live_mount_id(db_name, host):
    oracle_live_mounts = rubrik.get('internal', '/oracle/db/mount?source_database_name={}'.format(db_name))
    host_id = get_oracle_host_id(host)
    # print("Debug: Host id is {}".format(host_id))
    host_id = host_id.split(':::')[1]
    id = []
    for mount in oracle_live_mounts['data']:
        if host_id == mount['targetHostId']:
            id.append(mount['id'])
    return id


def get_oracle_host_id(hostname):
    host_info = rubrik.get('internal', '/oracle/host?name={}'.format(hostname))
    host_id = ''
    if host_info['total'] == 0:
        host_id = get_oracle_rac_id(hostname)
        if not host_id:
            print("Host not found nor is it part of a RAC cluster.")
            exit(1)
    elif host_info['total'] > 1:
        print(host_info['total'])
        print("Multiple Host IDs found:")
        for hosts in host_info['data']:
            print("Host: {}, ID: {}.".format(hosts['name'], hosts['id']))
        exit(1)
    else:
        host_id = host_info['data'][0]['id']
    return host_id



