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
@click.argument('live_mount_db_name')
@click.argument('live_mount_host_name')
@click.option('--force', '-f', is_flag=True, help='Force unmount')
def cli(live_mount_db_name, live_mount_host_name, force):
    cluster_info = get_cluster_info()
    timezone = cluster_info['timezone']['timezone']
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    live_mount_ids = get_oracle_live_mount_id(live_mount_db_name, live_mount_host_name)
    if not live_mount_ids:
        print("No live mounts found for {} live mounted on {}. ".format(live_mount_db_name, live_mount_host_name))
    else:
        for live_mount_id in live_mount_ids:
            unmount_info = rubrik.delete('internal', '/oracle/db/mount/{}?force={}'.format(live_mount_id,force))
            print("Live mount id: {} Unmount status: {}.".format(live_mount_id, unmount_info['status']))


def get_cluster_info():
    cluster_info = rubrik.get('v1','/cluster/me')
    return cluster_info


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
