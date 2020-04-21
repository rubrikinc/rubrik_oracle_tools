# rubrik_oracle_tools
Utilities for working with Rubrik RBS Oracle backups.

Rubrik Oracle Tools Python Scripts

Some basic command line python scripts for managing Rubrik Oracle RBS backups.

These scripts require python 3.7 or greater. This is generally not installed on most system and will need to be installed.

# :hammer: Installation
## Python 3.7 installation instructions for OEL/RHEL linux. 
------------------------------------------------------------
As root:

```
yum install gcc openssl-devel bzip2-devel libffi-devel
cd /usr/src
wget https://www.python.org/ftp/python/3.7.6/Python-3.7.6.tgz
tar xzf Python-3.7.6.tgz
cd Python-3.7.6
./configure --enable-optimizations
make altinstall
rm /usr/src/Python-3.7.6.tgz
```
Now check python:

```
python3.7 -V
```
Python 3.7 is now installed.


## Create a python virtual environment to Run the scripts (optional)
------------------------------------------------------------------------------------
As the user (oracle):
```
cd to where you want the env
cd /home/oracle/rubrik_oracle_tools/
python3.7 -m venv venv37
```

Activate the environment (This can be added to your .bash_profile):
```
source venv37/bin/activate
```

Upgrade pip (optional):
```
pip install --upgrade pip
```

## Install the Rubrik Oracle scripts
------------------------------------------------
cd to the Rubrik Oracle Tools directory
```
cd /home/oracle/rubrik_oracle_tools/
```

Install the module with setup tools:
```
pip install --editable .
```

## 	:gear: Configure the connection parameters
----------------------------------------------------
Edit the config.json file with the Rubrik CDM connection parameters or set those parameters as environmental variable (see instructions at build.rubrik.com)
You must provide the Rubrik CDM address or an IP in the cluster and either an API token or a user/password.

#### Example config.json file:
```
{
  "rubrik_cdm_node_ip": "10.1.1.20",
  "rubrik_cdm_token": "",
  "rubrik_cdm_username": "oraclesvc",
  "rubrik_cdm_password": "RubrikRules"
}
```
You should probably restrict access to the config.json file
```
chmod 600 config.json
```

## :mag: Available commands:
----------------------------------------------------
#### rubrik_oracle_backup_info
```
rubrik_oracle_backup_info --help
Usage: rubrik_oracle_backup_info [OPTIONS] HOST_CLUSTER_DB

  Displays information about the Oracle database object, the available
  snapshots, and recovery ranges.

  Args:     
  host_cluster_db (str): The hostname the database is running on : The database name
  Returns:     
  None: Information is printed to standard out

Options:
  --help  Show this message and exit.
```

#### rubrik_oracle_backup_mount
```
rubrik_oracle_backup_mount --help
Usage: rubrik_oracle_backup_mount [OPTIONS] HOST_CLUSTER_DB PATH

  This will mount the requested Rubrik Oracle backup set on the provided
  path.

  The source database is specified in a host:db format. The mount path is
  required. If the restore time is not provided the most recent recoverable
  time will be used. The host for the mount can be specified if it is not it
  will be mounted on the source host. If the source database is on a RAC
  cluster the target must be a RAC cluster. 
  
  Args:     
  host_cluster_db (str): The hostname the database is running on : The database name.     
  path (str): The path for the mount. This must exist on the requested host.
  time_restore (str): The point in time for the backup set in  iso 8601 format (2019-04-30T18:23:21).    
  target_host (str): The host to mount the backup set. If not specified the source host will be used. IF source DB in on RAC this must be a RAC Cluster.

  Returns:     
  live_mount_info (dict): The information about the requested files only mount returned from the Rubrik CDM.

Options:
  -t, --time_restore TEXT  Point in time to mount the DB, format is
                           YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15
  -h, --target_host TEXT   Host or RAC cluster name (RAC target required if
                           source is RAC)  for the Live Mount
  --help                   Show this message and exit.

```

#### rubrik_oracle_db_mount
```
rubrik_oracle_db_mount --help
Usage: rubrik_oracle_db_mount [OPTIONS] HOST_CLUSTER_DB TARGET_HOST

  Live mount a Rubrik Oracle Backup.

  Gets the backup for the Oracle database on the Oracle database host and
  will live mount it on the host provided.

  Args:     
  host_cluster_db (str): The hostname the database is running on : The database name    
  target_host (str): The host to live mount the database. (Must be a compatible Oracle host on Rubrik)     
  time_restore: The point in time for the live mount iso 8601 format (2019-04-30T18:23:21)

  Returns:     
  live_mount_info (json); JSON text file with the Rubrik cluster response to the live mount request

Options:
  -t, --time_restore TEXT  Point in time to mount the DB, iso format is
                           YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15
  --help                   Show this message and exit.

```

#### rubrik_oracle_unmount
```
rubrik_oracle_unmount --help
Usage: rubrik_oracle_unmount [OPTIONS] HOST_CLUSTER_DB

  This will unmount a Rubrik live mount using the database name and the live
  mount host.

  Args:     
  host_cluster_db (str): The hostname the database is running on : The database name     
  force (bool): Force the unmount

  Returns:     
  unmount_info (dict): Status of the unmount request.

Options:
  -f, --force  Force unmount
  --help       Show this message and exit.
```

#### rubrik_oracle_snapshot
```
rubrik_oracle_snapshot --help
Usage: rubrik_oracle_snapshot [OPTIONS] HOST_CLUSTER_DB

      This will initiate an on demand snapshot (backup) of the database.

      The source database is specified in a host:db format. To force a new full level 0
      image backup of the database set force to True. If you would like to use a different SLA for this snapshot you
      can specify that here also. Note if no SLA is supplied the current sla for this database will be used.

      Args:
          host_cluster_db (str): The hostname the database is running on : The database name.
          force (bool): Force a new full database image level 0 backup
          sla (str): The Rubrik SLA Domain to use if different than the assigned SLA

      Returns:
          snapshot_info (dict): The information about the snapshot returned from the Rubrik CDM.


Options:
  -f, --force     Force a new full database image level 0 backup
  -s, --sla TEXT  Rubrik SLA Domain to use if different than the assigned SLA
  --help          Show this message and exit.
```

#### rubrik_oracle_log_backup
```
rubrik_oracle_log_backup --help
Usage: rubrik_oracle_log_backup [OPTIONS] HOST_CLUSTER_DB

      This will initiate an on demand archive log backup of the database.

      The source database is specified in a host:db format.

      Args:
          host_cluster_db (str): The hostname the database is running on : The database name.

      Returns:
          log_backup_info (dict): The information about the snapshot returned from the Rubrik CDM.


Options:
  --help  Show this message and exit.
```

### rubrik_oracle_backup_mount_clone
```
 rubrik_oracle_backup_mount_clone --help
Usage: rubrik_oracle_backup_mount_clone [OPTIONS]

      This will mount the requested Rubrik Oracle backup set on the provided
      path.

      The source database is specified in a host:db format. The backup mount path is required. If the restore time is not
      provided the most recent recoverable time will be used. The host for the mount clone must be specified along with
      the directory for the temp, redo, etc. and the new database name. If the Oracle Home is not specified the ORACLE
      HOME path from the source database will be used. This is for a single instance database only, at present it will
      NOT work on RAC. It has not yet been tested with ASM.


Options:
  -s, --source_host_db TEXT   The source <host or RAC cluster>:<database>
                              [required]

  -m, --mount_path TEXT       The path used to mount the backup files
                              [required]

  -h, --host_target TEXT      Host or RAC cluster name (RAC target required if
                              source is RAC)  for the Live Mount.  [required]

  -n, --new_oracle_name TEXT  Name for the cloned live mounted database
                              [required]

  -f, --files_directory TEXT  Location for Oracle files written to the host,
                              control files, redo, etc.  [required]

  -o, --oracle_home TEXT      ORACLE_HOME path for this database clone
  -t, --time_restore TEXT     The point in time for the database clone in  iso
                              8601 format (2019-04-30T18:23:21)

  -d, --debug_level TEXT      Logging level: DEBUG, INFO, WARNING or CRITICAL.
  --help                      Show this message and exit.

```

### rubrik_oracle_db_mount_clone
```
 rubrik_oracle_db_mount_clone --help
Usage: rubrik_oracle_db_mount_clone [OPTIONS]

  Live mount an Oracle database from a Rubrik Oracle Backup and rename the
  live mounted database.

      Live mounts an Oracle database from the Rubrik backups. The database is then shutdown, mounted, and
      the name changed using the Oracle NID utility. Note that live mounted databases that have had the
      name changed will need to be cleaned up after the database is unmounted. The
      rubrik_oracle_db_clone_unmount utility will both unmount the live mount and cleanup the database
      files.

      Returns:
          live_mount_info (json); JSON text file with the Rubrik cluster response to the live mount request


Options:
  -s, --source_host_db TEXT   The source <host or RAC cluster>:<database>
                              [required]

  -h, --host_target TEXT      Host or RAC cluster name (RAC target required if
                              source is RAC)  for the Live Mount   [required]

  -n, --new_oracle_name TEXT  Name for the cloned database  [required]
  -t, --time_restore TEXT     Point in time to mount the DB, iso format is
                              YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15

  -d, --debug_level TEXT      Logging level: DEBUG, INFO, WARNING, ERROR or
                              CRITICAL.

  --help                      Show this message and exit.

```

### rubrik_oracle_mount_info
```
rubrik_oracle_mount_info --help
Usage: rubrik_oracle_mount_info [OPTIONS]

      This will unmount a Rubrik live mount using the database name and the
      live mount host.

      Returns:
          unmount_info (dict): Status of the unmount request.


Options:
  -s, --source_host_db TEXT  The source <host or RAC cluster>:<database>
                             [required]

  -m, --mounted_host TEXT    The host with the live mount to remove
                             [required]

  -d, --debug_level TEXT     Logging level: DEBUG, INFO, WARNING or CRITICAL.
  --help                     Show this message and exit.

```

### rubrik_oracle_clone_unmount
```
 rubrik_oracle_clone_unmount --help
Usage: rubrik_oracle_clone_unmount [OPTIONS]

  This will unmount a Rubrik live mount that has had the name changed after
  the live mount  using the the live mount host:Original DB Name, new Oracle
  DB name and the ORACLE_HOME

Options:
  -s, --source_host_db TEXT   The source <host or RAC cluster>:<database>
                              [required]

  -m, --mounted_host TEXT     The host with the live mount to remove
                              [required]

  -n, --new_oracle_name TEXT  Oracle database clone name. If unmounting more
                              than one separate with commas.  [required]

  -o, --oracle_home TEXT      ORACLE_HOME path for the mounted database(s) if
                              different than source database ORACLE_HOME

  -a, --all_mounts            Unmount all mounts from the source host:db.
                              Provide all the clone names separated by commas.

  -d, --debug_level TEXT      Logging level: DEBUG, INFO, WARNING or CRITICAL.
  --help                      Show this message and exit.

```
