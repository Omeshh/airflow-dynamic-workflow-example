import boto3


def get_client(service='rds', region_name='us-east-1'):
    return boto3.client(service, region_name=region_name)


def get_rds_ca(local_dir="/tmp", download_url="https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem"):
    import os.path

    local_filename = "{}/rds-combined-ca-bundle.pem".format(local_dir)

    if not os.path.isfile(local_filename):
        import requests
        with open(local_filename) as f:
            f.write(requests.get(download_url).content)
    return local_filename


def get_rds_credentials(rds_endpoint, user_name, port=5432, client=None):
    if not client:
        get_client()

    return client.generate_db_auth_token(rds_endpoint, port, user_name)
