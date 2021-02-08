"""
Script to initialize airflow variables and connections.
"""
import re
import os
import os.path as op
import json
import subprocess
import argparse
from cryptography.fernet import Fernet


parser = argparse.ArgumentParser()
parser.add_argument("environment", choices=['local', 'dev', 'prod'],
                    help="Environment name (local/dev/prod) for loading config files")
parser.add_argument('--skip_vars', action='store_true', default=False, help="Skip importing variables")
parser.add_argument('--skip_pools', action='store_true', default=False, help="Skip importing pools")
parser.add_argument('--del_conns', action='store_true', default=False, help="Delete connections before creating")
parser.add_argument('--skip_conns', action='store_true', default=False, help="Skip creating connections")
args = parser.parse_args()

airflow_home = os.getenv('AIRFLOW_HOME')
vars_file = op.join(airflow_home, 'config/variables-' + args.environment + '.json')
conn_folder = op.join(airflow_home, 'config', 'connections', args.environment)
conn_files = [f for f in os.listdir(conn_folder) if re.search(r'\.json$', f)] if os.path.isdir(conn_folder) else []
pools_folder = op.join(airflow_home, 'config', 'pools', args.environment)
pool_files = [f for f in os.listdir(pools_folder) if re.search(r'\.json$', f)] if os.path.isdir(pools_folder) else []
creds_file = op.join(airflow_home, 'keys/creds-' + args.environment + '.json')
key_file = op.join(airflow_home, 'keys/enc-key-' + args.environment + '.json')

with open(key_file) as f:
    decr = Fernet(json.load(f).get("key", "").encode('ascii', 'ignore'))


def run_bash_command(command, env=os.environ.copy()):
    """Runs bash command"""
    try:
        p = subprocess.run([command],
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True, shell=True, env=env)

    except subprocess.CalledProcessError as err:
        print(err.stdout.decode('ascii', 'ignore') + str(err))
        raise err
    else:
        print(p.stdout.decode('ascii', 'ignore'))


def configure_vars():
    """Imports variables"""
    import_vars_cmd = '/entrypoint.sh airflow variables -i ' + vars_file
    run_bash_command(import_vars_cmd)


def configure_pools():
    """Recreates pools"""

    for pool_file in pool_files:
        create_pool_cmd = '/entrypoint.sh airflow pool -i ' + op.join(pools_folder, pool_file)
        run_bash_command(create_pool_cmd)


def configure_conns(del_conns):
    """Deletes and recreates connections"""

    with open(op.join(airflow_home, 'config', 'connections', args.environment, creds_file)) as f:
        creds = json.load(f)

    for conn_file in conn_files:
        with open(op.join(conn_folder, conn_file)) as f:
            conn = json.load(f)

        if del_conns:
            delete_conn_cmd = '/entrypoint.sh airflow connections -d --conn_id=' + conn['conn_id']
            run_bash_command(delete_conn_cmd)

        create_conn_cmd = '/entrypoint.sh airflow connections -a '
        for conn_param, conn_value, in conn.items():
            if isinstance(conn_value, list):
                conn_value = ', '.join(conn_value)
                print(conn_value)
            token = creds.get(conn['conn_id'], "")
            if conn_param == 'conn_password' or '<password>' in conn_value:
                passs = decr.decrypt(token.encode('ascii', 'ignore')).decode('ascii', 'ignore')
                conn_value = conn_value.replace('<password>', passs)

            create_conn_cmd = create_conn_cmd + ' --' + conn_param + '=' + conn_value
        run_bash_command(create_conn_cmd)


if __name__ == "__main__":
    if not args.skip_vars:
        print("Creating Airflow variables..")
        configure_vars()

    if not args.skip_pools:
        print("Creating Airflow pools..")
        configure_pools()

    if not args.skip_conns:
        print("Creating Airflow connections..")
        configure_conns(args.del_conns)
