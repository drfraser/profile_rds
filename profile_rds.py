# Copyright 2012 Douglas Fraser   All Rights Reserved
#
# Amazon's RDS service allows one to modify any of the MySQL DBMS system
# parameters, such as innodb_buffer_pool.  But if you are not an expert DBA, it
# may not be clear what effect modifying a parameter, or group of parameters,
# may have. This script is a blueprint for automatically doing tests to see how
# changing those system parameters will affect the performance of the
# DBInstance, as well as changing the DBInstance class.
#
# Tests against localhost can be done using unittest -
#       python -m unittest profile_rds.BasicReportTests

# Things that need to be modified:
#   Settings at the top
#   test_sql()
#   load_data_sql()
#   PROFILES constant

import sys
import logging
import time
import threading
import unittest

import boto
import MySQLdb
import MySQLdb.cursors

DEFAULT_DATABASE = 'testdata'
DEFAULT_USER = 'testuser'
DEFAULT_PASSWD = 'testpass'
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 3306

# prefix to use in name of RDS instances and parameter groups
label = "testing"
inst_class = "db.t1.micro"

def test_sql():
    """Returns list of SQL statements to profile"""
    sql = []
    sql.append("set profiling=1")
    sql.append("select 1=1")
    return sql

def load_data_sql():
    """Returns list of SQL statements that load database"""
    sql = []
    sql.append("select 1=1")
    return sql

################

CPU_PROFILE = 'CPU'
CONTEXT_PROFILE = 'CONTEXT SWITCHES'
BLOCK_PROFILE = 'BLOCK IO'
IPC_PROFILE = 'IPC'
PAGE_PROFILE = 'PAGE FAULTS'
SWAPS_PROFILE = 'SWAPS'
SOURCE_PROFILE = 'SOURCE'
ALL_PROFILES = [CPU_PROFILE, CONTEXT_PROFILE, BLOCK_PROFILE, IPC_PROFILE,
                PAGE_PROFILE, SWAPS_PROFILE, SOURCE_PROFILE]
# default set of profiles to use
PROFILES = ALL_PROFILES

profile_headers = {'Always': (["", "Status", "Duration"], ["{:20}", "{:>10}"]),
    CPU_PROFILE: (["", "CPU User", "CPU System"], ["{:>10}", "{:>11}"]),
    CONTEXT_PROFILE:
        (["Context", "voluntary", "involuntary"], ["{:>10}", "{:>12}"]),
    BLOCK_PROFILE: (["Block", "ops in", "ops out"], ["{:>7}", "{:>8}"]),
    IPC_PROFILE: (["Messages", "sent", "received"], ["{:>9}", "{:>9}"]),
    PAGE_PROFILE:
        (["Page faults", "major", "minor"], ["{:>12}", "{:>12}"]),
    SWAPS_PROFILE: (["", "Swaps"], ["{:>6}"]),
    SOURCE_PROFILE: (["", "Source function", "Source file", "Source line"],
                     ["{:>18}", "{:>15}", "{:>12}"])
}


def connect(database=DEFAULT_DATABASE, username=DEFAULT_USER,
            pwd=DEFAULT_PASSWD, hostname=DEFAULT_HOST, portnum=DEFAULT_PORT,
            cc=MySQLdb.cursors.Cursor):
    """Connects to specified database

    Args:
        database:    Name of database
        username:    User account
        pwd:         Password
        hostname:    Hostname
        portnum:     Port
        cc:          MySQLdb.cursor type

    Returns:
        MySQLdb.Connection

    Raises:
        MySQLdb exceptions if MySQLdb.connect() fails

    """
    try:
        dbc = MySQLdb.connect(host=hostname, user=username, passwd=pwd,
                db=database, cursorclass=cc, port=portnum, use_unicode=True,
                charset="utf8")
    except:
        raise
    else:
        dbc.autocommit(True)
        return dbc


def add_utf8_support(param_set):
    """For extending parameter set with standard utf8 settings

    Args:
        param_set   list of parameters to add utf8 ones to

    """
    utf8params = [('character_set_server', 'utf8'),
                  ('character_set_client', 'utf8'),
                  ('character_set_connection', 'utf8'),
                  ('character_set_database', 'utf8'),
                  ('character_set_results', 'utf8'),
                  ('collation_server', 'utf8_general_ci'),
                  ('collation_connection', 'utf8_general_ci')]
    param_set.extend(utf8params)


def create_param_groups(name_prefix, params_to_vary, engine='MySQL5.5'):
    """Creates new sets of parameter groups based on list

    Args:
        name_prefix        prefix of name for new parameter groups
                           A numerical index is appended to the name
        params_to_vary     list of list of tuples, each tuple being a
                           specific DB parameter and its value
        engine             Version of MySQL to use

    An empty list for a parameter set will create a parameter
    group based on the default Amazon parameter group

    Returns:
        list of names of parameter groups

    """
    rds = boto.connect_rds()
    retVal = []

    def set_param(param, val):
        pg = rds.get_all_dbparameters(pg_name)
        if param in pg:
            pg[param].value = val
            pg[param].apply()
        else:
            pg2 = rds.get_all_dbparameters(pg_name, marker=pg.Marker)
            pg2[param].value = val
            pg2[param].apply()

    for index, param_set in enumerate(params_to_vary):
        pg_name = "pg{}-{}".format(name_prefix, index)
        new_param_group = rds.create_parameter_group(pg_name, engine=engine,
                                description='{} {}'.format(name_prefix, index))
        if isinstance(param_set, list):
            for pval in param_set:
                set_param(*pval)
        else:
            set_param(*param_set)

        retVal.append(new_param_group.name)
    return retVal


def create_db(number, label, inst_class, param_grp):
    """Creates a RDS instance

    Args:
        number          index number of RDS instance
        label           prefix to apply to name
        inst_class      RDS class to sue
        param_grp       param group to use

    Returns:
        RDS instance

    """
    rds = boto.connect_rds()
    logging.info("Creating RDS instance "
                 "with default database: {} {} {}\n".format(label, inst_class,
                                                            param_grp))
    inst = rds.create_dbinstance(id='{}-{}-{}'.format(label, number, param_grp),
                                 allocated_storage=10,
                                 instance_class=inst_class,
                                 master_username='root',
                                 master_password='changeME',
                                 param_group=param_grp,
                                 backup_retention_period='0',
                                 preferred_backup_window='01:00-02:00',
                                 engine_version='5.5',
                                 security_groups=['default'])
    time.sleep(30)
    inst.update()
    while inst.status != 'available':
        time.sleep(30)
        inst.update()
    db = connect(database='', username='root', pwd='changeME',
                 hostname=inst.endpoint[0])
    c = db.cursor()
    try:
        c.execute("CREATE DATABASE {}".format(DEFAULT_DATABASE), None)
        c.execute("grant all on {}.* to ".format(DEFAULT_DATABASE) + \
                  "{}@'%' identified by '{}'".format(DEFAULT_USER,
                                                     DEFAULT_PASSWD))
    except:
        logging.error("setting up {}, error {} {}\n".format(
            '{}-{}-{}'.format(label, number, param_grp),
                                                    db.errno(), db.error()))
    finally:
        c.close()
        db.close()
    return inst


def db_status():
    """Returns list of current RDS instances"""
    rds = boto.connect_rds()
    rs = rds.get_all_dbinstances()
    if rs:
        for inst in rs:
            logging.debug('RDS instance {}, status: {}, endpoint: {}'.format(
                                        inst.id, inst.status, inst.endpoint))
    else:
        logging.debug('No RDS instances')
    return rs


def cleanup(label, pgroups):
    """Deletes RDS instances, waiting until they don't exist or timeout

    Args:
        label       Label searched for in RDS instance IDs
        pgroups     list of parameter group names

    """
    rds = boto.connect_rds()
    loop = 0
    label_rs = True
    while loop < 10 and label_rs:
        rs = rds.get_all_dbinstances()
        label_rs = [d for d in rs if label in d.id]
        for inst in label_rs:
            if inst.status in ['available', 'failed', 'storage-full',
                               'incompatible-option-group',
                               'incompatible-parameters',
                               'incompatible-restore',
                               'incompatible-network']:
                logging.info("Deleting RDS instance {}".format(inst.id))
                rds.delete_dbinstance(inst.id, skip_final_snapshot=True)
        if label_rs:
            time.sleep(60)
        loop += 1
    if loop == 10 and rs:
        logging.error("Problem deleting RDS instances: timed out")
    else:
        for pg in pgroups:
            rds.delete_parameter_group(pg)


def load_db(number, label, param_grp):
    """Loads a specific database with test data

    Args:
        number          index number of RDS instance
        label           prefix to apply to name
        param_grp       param group to use

    """
    rds = boto.connect_rds()
    logging.info("Loading data into database: {} {} {}\n".format(label, number,
                                                                 param_grp))
    inst = rds.get_all_dbinstances(instance_id='{}-{}-{}'.format(label, number,
                                                                 param_grp))
    db = connect(hostname=inst[0].endpoint[0])
    c = db.cursor()

    sql = load_data_sql()
    try:
        for s in sql:
            c.execute(s, None)
    except:
        logging.error("load data error on {} {} {}\n".format(
            '{}-{}-{}'.format(label, number, param_grp),
                                                    db.errno(), db.error()))
    finally:
        c.close()
        db.close()


def profile_report(sql, rows, categories):
    """Logs formatted profiling information

    Args:
        sql         The SQL statement
        rows        Profiling info
        categories  Categories of profiling info gotten from DBMS

    MySQL always returns profile fields in a specific order - see ALL_PROFILES

    """
    logging.info("\nSQL: {}".format(sql))
    logging.info("-" * 20)
    header_keys = ['Always']
    header_keys.extend(categories)
    # do first line of headers - format specs used to set width of col
    formats = []
    map(lambda a: formats.extend(profile_headers[a][1]), header_keys)
    format_string = " ".join(formats)
    headers = []
    map(lambda a: headers.extend([profile_headers[a][0][0]] *
                                 len(profile_headers[a][1])), header_keys)
    logging.info(format_string.format(*headers))

    formats = []
    map(lambda a: formats.extend(profile_headers[a][1]), header_keys)
    format_string = " ".join(formats)
    headers = []
    map(lambda a: headers.extend(profile_headers[a][0][1:]), header_keys)
    logging.info(format_string.format(*headers))

    for row in rows:
        str_row = [str(x) for x in row]
        logging.info(format_string.format(*str_row))


def perform_test(db, profiles, db_name):
    """Profiles list of sql statements

    Args:
        db          database connection to RDS or localhost etc
        profiles    list of *_PROFILE constants
        db_name    Name of database or RDS instance

    Returns:
        logging output contains formatted report

    """
    c = db.cursor()
    profile_list = ",".join(profiles)
    sql = test_sql()
    try:
        for s in sql:
            c.execute(s, None)

        c.execute("show profiles", None)
        rows = c.fetchall()
        logging.info("Database {} Profiling data\n".format(db_name))
        logging.info("{:>10}  {:>12}  {}".format("Query ID", "Duration",
                                                 "Query"))
        logging.info("{}".format("-" * 31))
        for row in rows:
            logging.info("{:>10}  {:>12.8f}  {}".format(row[0], row[1], row[2]))
        for i, s in enumerate(sql):
            if i == 0:
                continue
            c.execute("show profile {} for query {}".format(profile_list, i),
                                                                        None)
            rows = c.fetchall()
            profile_report(s, rows, profiles)

        c.execute("set profiling=0", None)
    except:
        raise
    finally:
        c.close()


def perform_rds_test(number, label, param_grp, profiles):
    """Performs a test against a specific RDS instance and logs the elapsed time

    Args:
        number          index number of RDS instance
        label           prefix to apply to name
        param_grp       param group to use
        profiles        list of profile types

    """
    rds = boto.connect_rds()
    logging.info("Performing test on database: {} {} {}\n".format(label, number,
                                                                 param_grp))
    inst = rds.get_all_dbinstances(instance_id='{}-{}-{}'.format(label, number,
                                                                 param_grp))
    try:
        db = connect(hostname=inst[0].endpoint[0])
        perform_test(db, profiles, inst[0].id)
    except Exception as e:
        logging.error("test error on {} {} {}\n".format(
            '{}-{}-{}'.format(label, number, param_grp),
                                                    db.errno(), db.error()))
        raise
    finally:
        db.close()


class BasicReportTests(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(stream=sys.stdout, format="%(message)s",
                            level=logging.INFO)

    def test_report(self):
        db = connect(database="database", hostname="localhost",
                         username="user", pwd="passwd")
        perform_test(db, ALL_PROFILES, "localhost")
        db.close()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, format="%(message)s",
                        level=logging.INFO)
    parameters = [[],
         [('innodb_buffer_pool_size', 100 * 1024 * 1024),
          ('max_heap_table_size', 100 * 1024 * 1024),
          ('tmp_table_size', 100 * 1024 * 1024)]]
    # default RDS parameter group uses latin1 as default char set etc
    for pg in parameters:
        add_utf8_support(pg)
    pgroups = create_param_groups(label, parameters)

    workers = []
    for i, pg in enumerate(pgroups):
        t = threading.Thread(name="create-RDS-{}".format(i),
                             target=create_db,
                             args=(i, label, inst_class, pg))
        workers.append(t)
        t.daemon = True
        t.start()
    dead_workers = [t.join() for t in workers if t.isAlive()]

    workers = []
    for i, pg in enumerate(pgroups):
        t = threading.Thread(name="load-RDS-{}".format(i),
                             target=load_db,
                             args=(i, label, pg))
        workers.append(t)
        t.daemon = True
        t.start()
    dead_workers = [t.join() for t in workers if t.isAlive()]

    workers = []
    for i, pg in enumerate(pgroups):
        t = threading.Thread(name="test-RDS-{}".format(i),
                             target=perform_rds_test,
                             args=(i, label, pg, PROFILES))
        workers.append(t)
        t.daemon = True
        t.start()
    dead_workers = [t.join() for t in workers if t.isAlive()]

    cleanup(label, pgroups)
