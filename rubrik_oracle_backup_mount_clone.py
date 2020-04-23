
import rbs_oracle_common
import click
import logging
import sys
import os
import platform
import datetime
import pytz
from subprocess import PIPE, Popen


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--mount_path', '-m', type=str, required=True, help='The path used to mount the backup files')
@click.option('--host_target', '-h', type=str, required=True, help='Host or RAC cluster name (RAC target required if source is RAC)  for the Live Mount.')
@click.option('--new_oracle_name', '-n', type=str, required=True, help='Name for the cloned live mounted database')
@click.option('--files_directory', '-f', type=str, required=True, help='Location for Oracle files written to the host, control files, redo, etc.')
@click.option('--oracle_home', '-o', type=str, help='ORACLE_HOME path for this database clone')
@click.option('--time_restore', '-t', type=str, help='The point in time for the database clone in  iso 8601 format (2019-04-30T18:23:21)')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(source_host_db, mount_path, time_restore, host_target, oracle_home, new_oracle_name, files_directory, debug_level):
    """
    This will mount the requested Rubrik Oracle backup set on the provided path.

\b
    The source database is specified in a host:db format. The backup mount path is required. If the restore time is not
    provided the most recent recoverable time will be used. The host for the mount clone must be specified along with
    the directory for the temp, redo, etc. and the new database name. If the Oracle Home is not specified the ORACLE
    HOME path from the source database will be used. This is for a single instance database only, at present it will
    NOT work on RAC.
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

    # Make sure this is being run on the target host
    if host_target.split('.')[0] != platform.uname()[1].split('.')[0]:
        raise RubrikOracleBackupMountCloneError("This program must be run on the target host: {}".format(host_target))
    if len(new_oracle_name) > 8:
        raise RubrikOracleBackupMountCloneError("The new oracle name: {} is too long. Oracle names must be 8 characters or less.".format(new_oracle_name))
    rubrik = rbs_oracle_common.RubrikConnection()
    source_host_db = source_host_db.split(":")
    database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, source_host_db[1], source_host_db[0])
    oracle_db_info = database.get_oracle_db_info()
    # If the source database is on a RAC cluster the target must be a RAC cluster otherwise it will be an Oracle Host
    if 'racName' in oracle_db_info.keys():
        if oracle_db_info['racName']:
            host_id = database.get_rac_id(rubrik.cluster_id, host_target)
    else:
        host_id = database.get_host_id(rubrik.cluster_id, host_target)
    # Use the provided time or if no time has been provided use the the most recent recovery point
    if time_restore:
        time_ms = database.epoch_time(time_restore, rubrik.timezone)
        logger.info("Using {} for mount.". format(time_restore))
    else:
        logger.info("Using most recent recovery point for mount.")
        time_ms = database.epoch_time(oracle_db_info['latestRecoveryPoint'], rubrik.timezone)
    # Check ORACLE_HOME and set to source ORACLE_HOME is not provided
    if not oracle_home:
        oracle_home = oracle_db_info['oracleHome']
    if not os.path.exists(oracle_home):
        raise RubrikOracleBackupMountCloneError("The ORACLE_HOME: {} does not exist on the target host: {}".format(oracle_home, host_target))
    # Get directories in path to allow us to find the new directory after the mount
    live_mount_directories = os.listdir(mount_path)
    logger.warning("Starting the mount of the requested {} backup pieces on {}.".format(source_host_db[1], host_target))
    live_mount_info = database.live_mount(host_id, time_ms, files_only=True, mount_path=mount_path)
    live_mount_info = database.async_requests_wait(live_mount_info['id'], 20)
    logger.info("Async request completed with status: {}".format(live_mount_info['status']))
    if live_mount_info['status'] != "SUCCEEDED":
        raise RubrikOracleBackupMountCloneError("Mount of backup files did not complete successfully. Mount ended with status {}".format(live_mount_info['status']))
    logger.warning("Live mount of the backup files completed.")
    # Now determine the new live mount directory
    new_live_mount_directories = os.listdir(mount_path)
    live_mount_directory = list(set(new_live_mount_directories) - set(live_mount_directories))
    if len(live_mount_directory) == 1:
        backup_path = os.path.join(mount_path, live_mount_directory[0])
    else:
        raise RubrikOracleBackupMountCloneError("Multiple directories were created in {} during this operation. Live mount directory cannot be determined".format(mount_path))
    logger.info("Using the live mount path: {}".format(backup_path))
    auto_backup_file = database.get_latest_autobackup(backup_path)
    # Create the directory for the Oracle files (redo, temp, etc)
    oracle_files_path = os.path.join(files_directory, new_oracle_name)
    logger.debug("Creating Oracle files directory {} if not present.".format(oracle_files_path))
    os.makedirs(oracle_files_path, exist_ok=True)
    # Create the audit directory
    audit_dir = os.path.join(oracle_files_path, 'adump')
    logger.debug("Creating audit dump directory {} if not present.".format(oracle_files_path))
    os.makedirs(audit_dir, exist_ok=True)
    # Create the FRA directory
    fast_recovery_area = os.path.join(oracle_files_path, 'fast_recovery_area')
    logger.debug("Creating fast recovery area directory {} if not present.".format(fast_recovery_area))
    os.makedirs(fast_recovery_area, exist_ok=True)
    # Create the temporary pfile to start Oracle
    init_file = os.path.join(oracle_home, 'dbs', 'init{}.ora'.format(new_oracle_name))
    logger.debug("Creating new temporary init file {}".format(init_file))
    with open(init_file, 'w') as file:
        file.write('db_name={}\n'.format(source_host_db[1]))
    logger.debug("Setting env variable ORACLE_HOME={}, ORACLE_SID={}.".format(oracle_home, new_oracle_name))
    os.environ["ORACLE_HOME"] = oracle_home
    os.environ["ORACLE_SID"] = new_oracle_name
    logger.warning("Restoring and configuring server parameter file.")
    logger.info(database.sqlplus_sysdba(oracle_home, "startup force nomount pfile='{}'".format(init_file)))
    logger.info(database.rman(oracle_home, "restore spfile from '{}';".format(auto_backup_file)))
    logger.info("Setting parameters in spfile before starting instance.")
    spfile = os.path.join(oracle_home, 'dbs', 'spfile{}.ora'.format(new_oracle_name))
    logger.info(database.sqlplus_sysdba(oracle_home, "alter system set spfile='{}';".format(spfile)))
    logger.info(database.sqlplus_sysdba(oracle_home, "alter system set audit_file_dest='{}' scope=spfile;".format(audit_dir)))
    logger.info(database.sqlplus_sysdba(oracle_home, "alter system set db_unique_name='{}' scope=spfile;".format(new_oracle_name)))
    logger.info(database.sqlplus_sysdba(oracle_home, 'startup force nomount;'))
    logger.info(database.rman(oracle_home, "restore controlfile to '{0}/control01.ctl' from '{1}';".format(oracle_files_path, auto_backup_file)))
    logger.info(database.sqlplus_sysdba(oracle_home, "alter system set control_files = '{}/control01.ctl' scope=spfile;".format(oracle_files_path)))
    logger.info(database.sqlplus_sysdba(oracle_home, "alter system set audit_file_dest = '{}' scope=spfile;".format(audit_dir)))
    logger.info(database.sqlplus_sysdba(oracle_home, "alter system set db_recovery_file_dest = '{}' scope=spfile;".format(fast_recovery_area)))
    logger.info(database.sqlplus_sysdba(oracle_home, 'startup force mount;'))
    logger.warning("Cataloging the backup files.")
    logger.info(database.rman(oracle_home, 'crosscheck copy; crosscheck backup; delete noprompt expired copy; delete noprompt expired backup;'))
    logger.info(database.rman(oracle_home, "catalog start with '{}' noprompt;".format(backup_path)))
    logger.warning("Switching to the Rubrik mounted data files.")
    logger.info(database.rman(oracle_home, 'switch database to copy;'))
    logger.warning("Setting redo log location.")
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
    logger.info(database.sqlplus_sysdba(oracle_home, move_redo_sql))
    logger.warning("Setting temporary tablespace location.")
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
    logger.info(database.sqlplus_sysdba(oracle_home, move_temp_sql))
    logger.warning("Recovering the Database.")
    # Fix the time format for Oracle if set and recover the database
    if time_restore:
        time_restore = time_restore.replace("T", "")
        logger.info(database.rman(oracle_home, """recover database until time "TO_DATE('{}','YYYY-MM-DD HH24:MI:SS')"; """.format(time_restore)))
    else:
        logger.info(database.rman(oracle_home, "recover database;"))
    logger.warning("Switching to no archive log mode.")
    logger.info(database.sqlplus_sysdba(oracle_home, 'alter database noarchivelog;'))
    logger.warning("Switching to new database name.")
    logger.info(database.sqlplus_sysdba(oracle_home, 'alter database open resetlogs;'))
    logger.info(database.sqlplus_sysdba(oracle_home, 'shutdown immediate;'))
    logger.info(database.sqlplus_sysdba(oracle_home, 'startup mount'))
    logfile = oracle_files_path + '/nid_' + new_oracle_name + '.log'
    logger.info("NID Logfile: {}".format(logfile))
    session = Popen([os.path.join(oracle_home, 'bin', 'nid'), 'target=/', 'dbname={}'.format(new_oracle_name), 'logfile={}'.format(logfile)], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    stdout, stderr = session.communicate()
    nid_return = "NID standard out: {}, standard error: {}.".format(stdout.decode(), stderr.decode())
    logger.info(nid_return)
    logger.info(database.sqlplus_sysdba(oracle_home, 'startup force nomount;'))
    logger.info(database.sqlplus_sysdba(oracle_home, "alter system set db_name='{}' scope=spfile;".format(new_oracle_name)))
    logger.info(database.sqlplus_sysdba(oracle_home, 'shutdown immediate;'))
    logger.info(database.sqlplus_sysdba(oracle_home, 'startup mount'))
    logger.info(database.sqlplus_sysdba(oracle_home, 'alter database open resetlogs;'))
    logger.warning("Database live mount complete")
    return


class RubrikOracleBackupMountCloneError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
