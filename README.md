# airflow-docker

## Requirement

- Python 3.6 installed (suggest to create virtual-env)
- Docker

## Development in PyCharm

- Make sure python version is correct 3.6.x `python3 --version`
- Make sure virtualenv is installed `pip3 install virtualenv` `virtualenv --version`
- Create and activate virtualenv

```
virtualenv -p `which python3` venv (in windows, virtualenv venv)
source venv/bin/activate (or on windows, venv\scripts\activate)
pip install -r requirements.txt
```


P.S.: For Mac, you will have to execute following commands before doing pip install -r requirements.txt

      - export SLUGIFY_USES_TEXT_UNIDECODE=yes
      - pip install cython
      - brew install freetds

- Open the project in PyCharm, mark `dags` and `tests` as source root.
By default, pycharm should detect that venv, and use that is interperter.
If not, you can change that in Preference-> Project -> project interpreter

Note: PyCharm can help on formatting, library importing, syntax checking, etc.
But in order to run the job, we create docker-compose for that.

## Run
1. Login to Artifactory using docker

   Use your user name and the api key instead of password. The api key can be found when you click the username on the top-right corner of the artifactory home screen. If you dont have access to artifactory, you can request it from HELPDESK.
   ```
   docker login https://artifactory-prod.domain.com
   ```
   After you get `Login Succeeded` you are good to continue.
1. Bring up docker containers
   ```
   docker-compose -f docker-compose-LocalExecutor.yml down
   docker-compose -f docker-compose-LocalExecutor.yml up -d
   ```
   For windows use `docker-compose-LocalExecutor-windows.yml`
1. Generate ecryption key. (First time only! If ran again, it will generate a new key and create a backup of the old in the same folder. That will make all local encrypted passwords obsolete)
   ```
   python scripts/keys.py -g
   ```
1. Use this key to encrypt passwords for local connections.
   ```
   python scripts/keys.py -e TestPassword
   ```
1. Copy the ecrypted value and place it in `keys/creds-local.json`

   If you need to check your password, you can decrypt by running:
   ```
   python scripts/keys.py -d AAAAAAAA==
   ```
1. Copy the needed cnnection files from `config\connections\dev` to `config\connections\local`
1. Run init configuration inside of docker container
   ```
   docker exec airflow_webserver_1 python /usr/local/airflow/scripts/airflow_setup.py local
   ```


## Test
```
export AIRFLOW_DAGS=dags
cd tests/
python -m unittest discover .
```
