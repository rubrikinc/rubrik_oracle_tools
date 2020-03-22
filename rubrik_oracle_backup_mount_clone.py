
import rubrik_oracle_module as rbk
import click
import datetime
import pytz
import os
from subprocess import PIPE, Popen

@click.command()
@click.argument('host_cluster_db')
@click.argument('path')
@click.option('--time_restore', '-t', type=str, help='Point in time to mount the DB, format is YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15')
@click.option('--target_host', '-h', type=str, help='Host or RAC cluster name (RAC target required if source is RAC)  for the Live Mount ')
@click.option('--oracle_home', '-o', type=str, help='ORACLE_HOME path for this database')
@click.option('--new_oracle_name', '-n', type=str, help='Name for the cloned database')
@click.option('--files_directory', '-f', type=str, help='Location for files written to the host, control files, redo, etc.')
def cli(host_cluster_db, path, time_restore, target_host, oracle_home, new_oracle_name, files_directory):
    """
    This will mount the requested Rubrik Oracle backup set on the provided path.

\b
    The source database is specified in a host:db format. The mount path is required. If the restore time is not
    provided the most recent recoverable time will be used. The host for the mount can be specified if it is not it
    will be mounted on the source host. If the source database is on a RAC cluster the target must be a RAC cluster.
\b
    Args:
        host_cluster_db (str): The hostname the database is running on : The database name.
        path (str): The path for the mount. This must exist on the requested host.
        time_restore (str): The point in time for the backup set in  iso 8601 format (2019-04-30T18:23:21).
        target_host (str): The host to mount the backup set. If not specified the source host will be used.
                            IF source DB in on RAC this must be a RAC Cluster.
        oracle_home (str): The ORACLE_HOME on the host where there live mount is being done.
        new_oracle_name (str): The new name for the live mounted database.
        files_directory (str): Path on the host where the Oracle files such as the control files, redo logs, temp, etc.
\b
    Returns:
        live_mount_info (dict): The information about the requested files only mount returned from the Rubrik CDM.
    """
    rubrik = rbk.connect_rubrik()
    cluster_info = rbk.get_cluster_info(rubrik)
    timezone = cluster_info['timezone']['timezone']
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(cluster_info['name'], cluster_info['version'], timezone))
    host_cluster_db = host_cluster_db.split(":")
    oracle_db_id = rbk.get_oracle_db_id(rubrik, host_cluster_db[1], host_cluster_db[0])
    oracle_db_info = rbk.get_oracle_db_info(rubrik, oracle_db_id)
    # If not target host is provide mount the backup pieces on the source database host
    if not target_host:
        target_host = host_cluster_db[0]
    # If the source database is on a RAC cluster the target must be a RAC cluster otherwise it will be an Oracle Host
    if 'racName' in oracle_db_info.keys():
        if oracle_db_info['racName']:
            host_id = rbk.get_rac_id(rubrik, cluster_info['id'], target_host)
    else:
        host_id = rbk.get_host_id(rubrik, cluster_info['id'], target_host)
    # Use the provided time or if no time has been provided use the teh most recent recovery point
    if time_restore:
        time_ms = rbk.epoch_time(time_restore, timezone)
        print("Using {} for mount.". format(time_restore))
    else:
        print("Using most recent recovery point for mount.")
        oracle_db_info = rbk.get_oracle_db_info(rubrik, oracle_db_id)
        time_ms = rbk.epoch_time(oracle_db_info['latestRecoveryPoint'], timezone)
    print("Starting the mount of the requested {} backup pieces on {}.".format(host_cluster_db[1], target_host))
    live_mount_info = rbk.live_mount(rubrik, oracle_db_id, host_id, time_ms, files_only=True, mount_path=path)
    cluster_timezone = pytz.timezone(timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(live_mount_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    print(live_mount_info['id'])    # debug
    print("Live mount status: {}, Started at {}.".format(live_mount_info['status'], start_time.strftime(fmt)))
    live_mount_info = rbk.request_status_wait_loop(rubrik, live_mount_info['id'], 'QUEUED', 10)
    live_mount_info = rbk.request_status_wait_loop(rubrik, live_mount_info['id'], 'RUNNING', 20)
    live_mount_info = rbk.request_status_wait_loop(rubrik, live_mount_info['id'], 'FINISHING', 10)
    print(live_mount_info)  # debug
    if rubrik.get('internal', '/oracle/request/{}'.format(live_mount_info['id']), timeout=60)['status'] != "SUCCEEDED":
        return live_mount_info
    auto_backup_file = rbk.get_latest_autobackup(path)
    with open(oracle_home + '/dbs/init{}.ora'.format(new_oracle_name), 'w') as file:
        file.write('db_name={}'.format(host_cluster_db[1]))
    os.environ["ORACLE_HOME"] = oracle_home
    os.environ["ORACLE_SID"] = new_oracle_name
    print(rbk.sqlplus_sysdba(oracle_home, 'startup nomount'))
    print(rbk.rman(oracle_home, "restore spfile from '{}';".format(auto_backup_file)))
    print(rbk.sqlplus_sysdba(oracle_home, "alter system set db_unique_name='{}' scope=spfile;".format(new_oracle_name)))
    print(rbk.sqlplus_sysdba(oracle_home, 'startup force nomount;'))
    oracle_files_path = files_directory + '/{}'.format(new_oracle_name)
    os.makedirs(oracle_files_path, exist_ok=True)
    print(rbk.rman(oracle_home, "restore controlfile to '{0}/control01.ctl' from '{1}';".format(oracle_files_path, auto_backup_file)))
    print(rbk.sqlplus_sysdba(oracle_home, "alter system set control_files = '{}/control01.ctl' scope=spfile;".format(oracle_files_path)))
    audit_dir = oracle_files_path + '/adump'
    os.makedirs(audit_dir, exist_ok=True)
    print(rbk.sqlplus_sysdba(oracle_home, "alter system set audit_file_dest = '{}' scope=spfile;".format(audit_dir)))
    print(rbk.sqlplus_sysdba(oracle_home, 'startup force mount;'))
    print(rbk.rman(oracle_home, 'crosscheck copy; crosscheck backup; delete noprompt expired copy; delete noprompt expired backup;'))
    print(rbk.rman(oracle_home, "catalog start with '{}' noprompt;".format(path)))
    print(rbk.rman(oracle_home, 'switch database to copy;'))
    # move the redo logs before we create them with an open resetlogs
    move_redo_sql = """
    SET SERVEROUTPUT ON
    DECLARE
        l_oracle_files_path VARCHAR2(50):= '{}';
        l_new_member VARCHAR2(60);
        l_sql_stmt VARCHAR2(200);
        CURSOR c_redo_files IS
        select member,
        substr(member,(instr(member,'/',-1,1) +1),length(member)) new_member
        from v$logfile;
        c_redo_files_var c_redo_files%ROWTYPE;
    BEGIN
        FOR c_redo_files_var in c_redo_files LOOP
           l_new_member := (l_oracle_files_path || '/' || c_redo_files_var.new_member);
           l_sql_stmt := 'alter database rename file ''' || c_redo_files_var.member || ''' to ''' || l_new_member || ''';' ;
           DBMS_OUTPUT.PUT_LINE(l_sql_stmt);
           EXECUTE IMMEDIATE 'alter database rename file ''' || c_redo_files_var.member || ''' to ''' || l_new_member || '''';
       END LOOP;
    END;
    / """.format(oracle_files_path)
    print(rbk.sqlplus_sysdba(oracle_home, move_redo_sql))

    # move the redo logs before we create them with an open resetlogs
    move_temp_sql = """
    SET SERVEROUTPUT ON
    DECLARE
        l_oracle_files_path VARCHAR2(50):= '{}';
        l_new_file VARCHAR2(60);
        l_sql_stmt VARCHAR2(200);
        CURSOR c_temp_files IS
        select name,
        substr(name,(instr(name,'/',-1,1) +1),length(name)) new_name
        from v$tempfile;
        c_temp_files_var c_temp_files%ROWTYPE;
    BEGIN
        FOR c_temp_files_var in c_temp_files LOOP
           l_new_file := (l_oracle_files_path || '/' || c_temp_files_var.new_name);
           l_sql_stmt := 'alter database rename file ''' || c_temp_files_var.name || ''' to ''' || l_new_file || ''';' ;
           DBMS_OUTPUT.PUT_LINE(l_sql_stmt);
           EXECUTE IMMEDIATE 'alter database rename file ''' || c_temp_files_var.name  || ''' to ''' || l_new_file || '''';
       END LOOP;
    END;
    / """.format(oracle_files_path)
    print(rbk.sqlplus_sysdba(oracle_home, move_temp_sql))

    print(rbk.rman(oracle_home, 'recover database;'))
    # Switch to noarchive log mode
    print(rbk.sqlplus_sysdba(oracle_home, 'alter database noarchive;'))
    # Now change the database name
    print(rbk.sqlplus_sysdba(oracle_home, 'alter database open resetlogs;'))
    print(rbk.sqlplus_sysdba(oracle_home, 'shutdown immediate;'))
    print(rbk.sqlplus_sysdba(oracle_home, 'startup mount'))
    logfile = oracle_files_path + '/nid_' + new_oracle_name + '.log'
    print("NID Logfile: {}".format(logfile))
    session = Popen(['nid', 'target=/', 'dbname={}'.format(new_oracle_name), 'logfile={}'.format(logfile)], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    stdout, stderr = session.communicate()
    print(stdout.decode(), stderr.decode())
    print(rbk.sqlplus_sysdba(oracle_home, 'startup force nomount;'))
    print(rbk.sqlplus_sysdba(oracle_home, "alter system set db_name='{}' scope=spfile;".format(new_oracle_name)))
    print(rbk.sqlplus_sysdba(oracle_home, 'shutdown immediate;'))
    print(rbk.sqlplus_sysdba(oracle_home, 'startup mount'))
    print(rbk.sqlplus_sysdba(oracle_home, 'alter database open resetlogs;'))

    print("Database live mount complete")
    return


class RubrikOracleBackupMountError(rbk.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
