import rbs_oracle_common
import click
import logging
import sys
import os
import platform
from datetime import datetime
import configparser


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True, help='The source <host or RAC cluster>:<database>')
@click.option('--rac_node_list', '-r', type=str, required=True,
              help='Provide the list of RAC DB clone nodes separated by :')
@click.option('--mount_path', '-m', type=str, required=True, help='The path used to mount the backup files')
@click.option('--new_oracle_name', '-n', type=str, required=True, help='Name for the cloned live mounted database')
@click.option('--configuration_file', '-f', type=str,
              help='Oracle duplicate configuration file, can be used for all optional parameters. Overrides any set as script options')
@click.option('--time_restore', '-t', type=str,
              help='The point in time for the database clone in  iso 8601 format (2019-04-30T18:23:21)')
@click.option('--oracle_home', '-o', type=str, help='ORACLE_HOME path for this database clone')
@click.option('--parallelism', '-p', default=4, type=str,
              help='The degree of parallelism to use for the RMAN duplicate')
@click.option('--no_spfile', is_flag=True,
              help='Restore SPFILE and replace instance specific parameters with new DB name')
@click.option('--no_file_name_check', is_flag=True,
              help='Do not check for existing files and overwrite existing files. Potentially destructive use with caution')
@click.option('--refresh_db', is_flag=True,
              help='Refresh and existing database. Overwriting exiting database. Requires no_file_name_check.')
@click.option('--control_files', type=str,
              help='Locations for control files. Using full paths in single quotes separated by commas')
@click.option('--db_file_name_convert', type=str,
              help='Remap the datafile locations. Using full paths in single quotes separated by commas in pairs of \'from location\',\'to location\'')
@click.option('--log_file_name_convert', type=str,
              help='Remap the redo log locations. Using full paths in single quotes separated by commas in pairs of \'from location\',\'to location\'')
@click.option('--audit_file_dest', type=str,
              help='Set the path for the audit files. This path must exist on the target host')
@click.option('--core_dump_dest', type=str,
              help='Set the path for the core dump files. This path must exist on the target host')
@click.option('--log_path', '-l', type=str, help='Log directory, if not specified the mount_path with be used.')
@click.option('--debug_level', '-d', type=str, default='WARNING',
              help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(source_host_db, rac_node_list, mount_path, new_oracle_name, configuration_file, time_restore, oracle_home,
        parallelism,
        no_spfile, no_file_name_check, refresh_db, control_files, db_file_name_convert, log_file_name_convert,
        audit_file_dest, core_dump_dest, log_path, debug_level):
    """
    This will use the Rubrik RMAN backups to do a duplicate (or refresh) of an Oracle Database.

\b
    The source database is specified in a host:db format. The backup mount path and the new Oracle DB name are required.
    RAC Node List  is specified in a racnode1:racnode2 format depending on the number of RAC nodes on which database has to be clone on a given RAC cluster nodes
    If the restore time is not provided the most recent recoverable time will be used. Command line Oracle path
    parameters must be enclosed in both double quotes and each path within enclosed with single quotes. All the optional
    parameters can be provided in a configuration file. All the flag options must be entered as true false in the
    configuration file. If the Oracle Home is not specified the ORACLE_HOME path from the source database will be used.
    If a log directory is not specified, no log will be created.
\b
Example:
rubrik_oracle_backup_clone -s jz-sourcehost-1:ora1db -r racnode1:racnode2 -m /u02/oradata/restore -n oracln -t 2020-11-06T00:06:00 -p 8
-l /home/oracle/clone_logs --no_file_name_check --refresh_db
--db_file_name_convert "'/u02/oradata/ora1db/','/u02/oradata/oracln/'"
--control_files "'/u02/oradata/oracln/control01.ctl','/u02/oradata/oracln/control02.ctl'"
--log_file_name_convert "'/u02/oradata/ora1db/','u02/oradata/oracln/'"
--audit_file_dest "'/u01/app/oracle/admin/clonedb/adump'"
--core_dump_dest "'/u01/app/oracle/admin/clonedb/cdump'"

\b
Example Configuration File:
### The following line is required:
[parameters]
### All parameters are optional. Command line flags are boolean (true/false)
### The degree of parallelism to use for the RMAN duplicate (default is 4)
# parallelism = 4
### Do not restore the spfile renaming the parameters with the new db name.
# no_spfile = true
### Pint in time for duplicate
# time_restore = 2020-11-08T00:06:00
### ORACLE_HOME if different than source db
# oracle_home = /u01/app/oracle/product/12.2.0/dbhome_1
### Do not check for existing files
# no_file_name_check = true
### Refresh an existing database. The database will be shutdown and the existing file will be overwritten.
### Requires no_file_name_check = True
# refresh_db = True
### Control File locations
# control_files = '/u02/oradata/clonedb/control01.ctl','/u02/oradata/clonedb/control02.ctl'
### Remap the database files
# db_file_name_convert = '/u02/oradata/ora1db/','/u02/oradata/clonedb/'
### Remap the redo log locations
# log_file_name_convert = '/u02/oradata/ora1db/','u02/oradata/clonedb/'
### Set the audit file destination path
# audit_file_dest = '/u01/app/oracle/admin/clonedb/adump'
### Set the core dump destination path
# core_dump_dest = '/u01/app/oracle/admin/clonedb/cdump'
### Directory where logs will be created. If not provided not logs will be created
# log_path = /home/oracle/clone_logs
\b
Example:
rubrik_oracle_backup_clone -s jz-sourcehost-1:ora1db -r racnode1,racnode2 -m /u02/oradata/restore -n oracln -f /home/oracle/clone_config.txt

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

    # Read in the configuration
    if configuration_file:
        configuration = configparser.ConfigParser()
        configuration.read(configuration_file)
        if 'parallelism' in configuration['parameters'].keys():
            parallelism = configuration['parameters']['parallelism']
        if 'no_spfile' in configuration['parameters'].keys():
            no_spfile = configuration['parameters'].getboolean('no_spfile')
        if 'oracle_home' in configuration['parameters'].keys():
            oracle_home = configuration['parameters']['oracle_home']
        if 'no_file_name_check' in configuration['parameters'].keys():
            no_file_name_check = configuration['parameters'].getboolean('no_file_name_check')
        if 'refresh_db' in configuration['parameters'].keys():
            refresh_db = configuration['parameters'].getboolean('refresh_db')
        if 'control_files' in configuration['parameters'].keys():
            control_files = configuration['parameters']['control_files']
        if 'db_file_name_convert' in configuration['parameters'].keys():
            db_file_name_convert = configuration['parameters']['db_file_name_convert']
        if 'log_file_name_convert' in configuration['parameters'].keys():
            log_file_name_convert = configuration['parameters']['log_file_name_convert']
        if 'log_path' in configuration['parameters'].keys():
            log_path = configuration['parameters']['log_path']
        if 'time_restore' in configuration['parameters'].keys():
            time_restore = configuration['parameters']['time_restore']
        if 'audit_file_dest' in configuration['parameters'].keys():
            audit_file_dest = configuration['parameters']['audit_file_dest']
        if 'core_dump_dest' in configuration['parameters'].keys():
            core_dump_dest = configuration['parameters']['core_dump_dest']
        logger.debug("Parameters for duplicate loaded from file: {}.".format(configuration))

    # Set up the file logging
    if log_path:
        os.makedirs(log_path, exist_ok=True)
        logfile = os.path.join(log_path,
                               "{}_Clone_{}.log".format(new_oracle_name, datetime.now().strftime("%Y%m%d-%H%M%S")))
        fh = logging.FileHandler(logfile, mode='w')
        fh.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s: %(message)s')
        fh.setFormatter(file_formatter)
        logger.addHandler(fh)

    source_host_db = source_host_db.split(":")
    rac_node_names = rac_node_list.split(",")
    rac_node_count = len(rac_node_names)

    racinst_dict = {}  # Initialize an empty dictionary
    for i, node_name in enumerate(rac_node_names, start=1):
        variable_name = f"{new_oracle_name}{i}"
        instance_number = i
        thread = i
        undo_tablespace = f"UNDOTBS{i}"

        variable_value = {
            "instance_number": instance_number,
            "thread": thread,
            "undo_tablespace": undo_tablespace
        }
        racinst_dict[variable_name] = variable_value

    # Get the target host name which is the host running the command
    host_target = platform.uname()[1].split('.')[0]
    logger.debug("The hostname used for the target host is {}".format(host_target))
    if len(new_oracle_name) > 8:
        logger.debug(
            "The new oracle name: {} is too long. Oracle names must be 8 characters or less. Aborting clone".format(
                new_oracle_name))
        raise RubrikOracleBackupMountCloneError(
            "The new oracle name: {} is too long. Oracle names must be 8 characters or less.".format(new_oracle_name))
    if new_oracle_name == source_host_db[1]:
        logger.debug("The new oracle db name {} cannot be the same as the source db name {} ".format(new_oracle_name,
                                                                                                     source_host_db[1]))
        raise RubrikOracleBackupMountCloneError(
            "The new oracle db name {} cannot be the same as the source db name {} ".format(new_oracle_name,
                                                                                            source_host_db[1]))

    rubrik = rbs_oracle_common.RubrikConnection()
    database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, source_host_db[1], source_host_db[0])
    oracle_db_info = database.get_oracle_db_info()
    host_id = database.get_any_rac_target_id(rubrik.cluster_id, host_target)
    # Use the provided time or if no time has been provided use the the most recent recovery point
    if time_restore:
        time_ms = database.epoch_time(time_restore, rubrik.timezone)
        logger.warning("Materializing backup set from time {} for mount.".format(time_restore))
    else:
        logger.warning("Using most recent recovery point for mount.")
        time_ms = database.epoch_time(oracle_db_info['latestRecoveryPoint'], rubrik.timezone)
    # Check ORACLE_HOME and set to source ORACLE_HOME is not provided
    if not oracle_home:
        if 'oracleHome' in oracle_db_info:
            oracle_home = oracle_db_info['oracleHome']
        else:
            logger.warning(
                "No oracle_home was provided and Rubrik does not know the oracle_home, most likely because the source is a Data Guard Group ")
    if not os.path.exists(oracle_home):
        logger.debug("The ORACLE_HOME: {} does not exist on the target host: {}".format(oracle_home, host_target))
        raise RubrikOracleBackupMountCloneError(
            "The ORACLE_HOME: {} does not exist on the target host: {}".format(oracle_home, host_target))
    # Get directories in path to allow us to find the new directory after the mount
    live_mount_directories = os.listdir(mount_path)
    logger.warning("Starting the mount of the requested {} backup pieces on {}.".format(source_host_db[1], host_target))
    live_mount_info = database.live_mount(host_id, time_ms, files_only=True, mount_path=mount_path)
    live_mount_info = database.async_requests_wait(live_mount_info['id'], 20)
    logger.debug("Backup Live Mount Asyc Request: {}".format(live_mount_info))
    logger.info("Async request completed with status: {}".format(live_mount_info['status']))
    if live_mount_info['status'] != "SUCCEEDED":
        logger.debug("Mount of backup files did not complete successfully. Mount ended with status {}".format(
            live_mount_info['status']))
        raise RubrikOracleBackupMountCloneError(
            "Mount of backup files did not complete successfully. Mount ended with status {}".format(
                live_mount_info['status']))
    logger.warning("Live mount of the backup files completed.")
    # Now determine the new live mount directory
    new_live_mount_directories = os.listdir(mount_path)
    live_mount_directory = list(set(new_live_mount_directories) - set(live_mount_directories))
    if len(live_mount_directory) == 1:
        backup_path = os.path.join(mount_path, live_mount_directory[0])
    else:
        logger.debug(
            "Multiple directories were created in {} during this operation. Live mount directory cannot be determined".format(
                mount_path))
        raise RubrikOracleBackupMountCloneError(
            "Multiple directories were created in {} during this operation. Live mount directory cannot be determined".format(
                mount_path))
    logger.info("Using the live mount path: {}".format(backup_path))
    live_mount_id = live_mount_directory[0].split('_')[1]
    logger.debug("Live mount ID is {}".format(live_mount_id))

    os.environ["ORACLE_HOME"] = oracle_home
    os.environ["ORACLE_SID"] = new_oracle_name
    logger.debug("Setting env variable ORACLE_HOME={}, ORACLE_SID={}.".format(oracle_home, new_oracle_name))

    if refresh_db:
        logger.warning("Shutting down {} database for refresh".format(new_oracle_name))
        logger.info(database.sqlplus_sysdba(oracle_home, "shutdown immediate;"))
    if no_spfile:
        logger.warning("Starting auxiliary instance")
        sql_return = database.sqlplus_sysdba(oracle_home, "startup nomount")
        logger.info(sql_return)
    else:
        logger.warning("Creating minimal init file to start instance")
        init_file = os.path.join(oracle_home, 'dbs', 'init{}.ora'.format(new_oracle_name))
        logger.debug("Creating new temporary init file {}".format(init_file))
        with open(init_file, 'w') as file:
            file.write('db_name={}\n'.format(new_oracle_name))
        logger.warning("Starting auxiliary instance")
        sql_return = database.sqlplus_sysdba(oracle_home, "startup nomount pfile='{}'".format(init_file))
        logger.info(sql_return)

    if "ORA-01081: cannot start already-running ORACLE" in sql_return:
        logger.debug(
            "There is an instance of {} all ready running on this host. Aborting clone".format(new_oracle_name))
        raise RubrikOracleBackupMountCloneError(
            "There is an instance of {} all ready running on this host or refreshed DB did not start cleanly. Aborting clone".format(
                new_oracle_name))
    sql_return = database.sqlplus_sysdba(oracle_home, "select instance_name from v$instance;")
    logger.info(sql_return)
    if new_oracle_name not in sql_return:
        logger.debug("DB Instance check failed. Instance name is not {}. Aborting clone".format(new_oracle_name))
        raise RubrikOracleBackupMountCloneError(
            "DB Instance check failed. Instance name is not {}. Aborting clone".format(new_oracle_name))

    logger.warning(
        "Beginning duplicate of {} to {} on host {}.".format(source_host_db[1], new_oracle_name, source_host_db[0]))
    duplicate_commands = "run { "
    for x in range(int(parallelism)):
        channel = x + 1
        duplicate_commands = duplicate_commands + "allocate auxiliary channel aux{} device type disk; ".format(channel)
    duplicate_commands = duplicate_commands + "duplicate database to '{}' ".format(new_oracle_name)
    if time_restore:
        time_restore = time_restore.replace("T", "")
        duplicate_commands = duplicate_commands + """until time "TO_DATE('{}','YYYY-MM-DD HH24:MI:SS')"  """.format(
            time_restore)
    if not no_spfile:
        duplicate_commands = duplicate_commands + "SPFILE parameter_value_convert ('{}','{}') ".format(
            source_host_db[1], new_oracle_name)
    if control_files:
        duplicate_commands = duplicate_commands + "set  control_files = {} ".format(control_files)
    if db_file_name_convert:
        duplicate_commands = duplicate_commands + "set  db_file_name_convert = {} ".format(db_file_name_convert)
    if log_file_name_convert:
        duplicate_commands = duplicate_commands + "set  log_file_name_convert = {} ".format(log_file_name_convert)
    if audit_file_dest:
        duplicate_commands = duplicate_commands + "set  audit_file_dest = {} ".format(audit_file_dest)
    if core_dump_dest:
        duplicate_commands = duplicate_commands + "set  core_dump_dest = {} ".format(core_dump_dest)
    duplicate_commands = duplicate_commands + "set cluster_database='FALSE'  "
    duplicate_commands = duplicate_commands + "BACKUP LOCATION '{}' ".format(mount_path)
    if no_file_name_check:
        duplicate_commands = duplicate_commands + "NOFILENAMECHECK; }"
    else:
        duplicate_commands = duplicate_commands + "; }"

    logger.debug("Duplicate script: "
                 "{}".format(duplicate_commands))
    logger.info(database.rman(oracle_home, duplicate_commands, "auxiliary"))
    logger.warning("Duplicate of non-RAC {} database complete.".format(new_oracle_name))

    ####Create temp init file and append RAC settings####
    logger.debug("Creating temporary init file")
    sql_return = database.sqlplus_sysdba(oracle_home, "create pfile='{}/dbs/rbktempinit{}.ora' from spfile;".format(oracle_home, new_oracle_name))
    logger.info(sql_return)
    temp_init_file = os.path.join(oracle_home, 'dbs', 'rbktempinit{}.ora'.format(new_oracle_name))
    logger.debug(f"Temp init file: {temp_init_file}")
    for instance in racinst_dict:
        logger.debug(f"Adding RAC instance parameters: {instance}.instance_name={racinst_dict[instance]['instance_number']}, "
                     f"{instance}.thread={racinst_dict[instance]['thread']}, "
                     f"{instance}.undo_tablespace={racinst_dict[instance]['undo_tablespace']} ")
    # Writing the dynamically generated RAC variables and *.cluster_database=TRUE to the temp_init_file in append mode
    with open(temp_init_file, 'a') as file:
        file.write("*.cluster_database=TRUE\n")  # Writing the additional line
        for variable_name, variable_value in racinst_dict.items():
            file.write(f"{variable_name}.instance_number = {variable_value['instance_number']}\n")
            file.write(f"{variable_name}.thread = {variable_value['thread']}\n")
            file.write(f"{variable_name}.undo_tablespace = '{variable_value['undo_tablespace']}'\n")
    ### Create spfile from temp_init_file with RAC settings in shared area
    sql_return = database.sqlplus_sysdba(oracle_home, f"create spfile='+DATA/{new_oracle_name}/PARAMETERFILE/spfile{new_oracle_name}.ora;' from pfile='{temp_init_file}'")
    logger.info(sql_return)
    ## Shutdown non-cluster database
    sql_return = database.sqlplus_sysdba(oracle_home, "shutdown immediate;")
    logger.info(sql_return)
    ###On Node1 , set Oracle_SID=new_oracle_name<1>
    ### Set ORACLE_SID to new_oracle_name1
    os.environ["ORACLE_SID"] = f"{new_oracle_name}1"
    logger.debug(f"Setting env variable ORACLE_SID={new_oracle_name}1")
    ### Construct the path to the init file with the "1" suffix on node1
    init_file_path = os.path.join(oracle_home, "dbs", f"init{new_oracle_name}1.ora")
    ### Check if the init file exists
    if os.path.exists(init_file_path):
        ### Create a backup filename by adding '.bak' extension
        backup_file_path = os.path.join(oracle_home, "dbs", f"init{new_oracle_name}1.ora.bak")
        ### Make a backup by copying the init file
        shutil.copy(init_file_path, backup_file_path)
        logger.debug(f"Backup of {init_file_path} created at {backup_file_path}")
    # Open the init file in write mode and write the line with variable
    sp_file_path = f"+DATA/{new_oracle_name}/PARAMETERFILE/spfile{new_oracle_name}.ora"  # Specify the sp_file_path\
    logger.debug(f"Creating init file: {init_file_path} with sp file path: {sp_file_path}")
    with open(init_file_path, 'w') as file:
        file.write(f"SPFILE='{sp_file_path}'\n")  # Writing the SPFILE line

    sql_return = database.sqlplus_sysdba(oracle_home, "startup;")
    logger.info(sql_return)
    sql_return = database.sqlplus_sysdba(oracle_home, f"{oracle_home}/rdbms/admin/catclust.sql;")
    logger.info(sql_return)
    sql_return = database.sqlplus_sysdba(oracle_home, "shutdown immediate;")
    logger.info(sql_return)

###### Add RAC Database to CRS Registry#####

###command = (
###f"{oracle_home}/bin/srvctl"
###f" add database -d {new_oracle_name} -o {oracle_home} -p {sp_file_path}"
###)

###try:
###exit_code = os.system(command)
###if exit_code == 0:
###print("Command executed successfully")
###else:
###print("Command execution failed")
###except Exception as e:
###print("Error:", e)


####Add RAC Instances to CRS registry in a loop based on node list#####
###for i, node_name in enumerate(rac_node_names, start=1):
###command = (
###f"{oracle_home}/bin/srvctl"
###f" add instance -d {oracle_new_name}"
###f" -i {oracle_new_name}{i}"
###f" -n {node_name}"
###)

###try:
###os.system(command)
###print(f"Command for node {node_name} executed successfully")
###except Exception as e:
###print(f"Error for node {node_name}: {e}")
#### Restart database using srvctl ##########
####shut_command = f"srvctl stop database -d {new_oracle_name}"
###  os.system(shut_command)
####start_command = f"srvctl start database -d {new_oracle_name}"
###  os.system(start_command)

    mount = rbs_oracle_common.RubrikRbsOracleMount(rubrik, source_host_db[1], source_host_db[0], host_target)
    logger.warning("Unmounting backups.")
    delete_request = mount.live_mount_delete(live_mount_id)
    delete_request = mount.async_requests_wait(delete_request['id'], 12)
    logger.info("Async request completed with status: {}".format(delete_request['status']))
    logger.debug(delete_request)
    if delete_request['status'] != "SUCCEEDED":
        logger.warning("Unmount of backup files failed with status: {}".format(delete_request['status']))
    else:
        logger.info("Live mount of backup data files with id: {} has been unmounted.".format(live_mount_id))
        logger.warning("Backups unmounted")

    logger.warning("Database clone complete")
    rubrik.delete_session()
    return


class RubrikOracleBackupMountCloneError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
