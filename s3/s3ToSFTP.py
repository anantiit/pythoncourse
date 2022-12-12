import os
import requests
import boto3
import paramiko
import json

from api_utils.env_base_url import get_base_url
from api_utils.OauthTokenManager import OauthTokenManager

TMP = "_tmp"


def lambda_handler(event, context):

    env = os.environ.get('MY_ENV')
    config = get_config(env, event)
    sftp_username = config.get('sftpServerUserName')
    sftp_host = config.get('sftpServerHostName')
    sftp_dir = config.get('sftpServerPdfReportDirectory')
    sftp_port = int(config.get('SSH_PORT', 22))
    sftp_password = config.get('sftpServerPassword')

    if config.get('pdfReportDelayedTransfer') == 'true':
        sftp_dir = config.get('sftpServerPdfReportDirectory') + TMP

    try:
        sftp, transport = connect_to_sftp(
            hostname=sftp_host,
            port=sftp_port,
            username=sftp_username,
            password=sftp_password,
        )
    except Exception as e:
        print("couldn't connect to SFTP due to error : {}".format(e))
    else:
        s3 = boto3.client('s3')
        if sftp_dir:
            sftp.chdir(sftp_dir)

        with transport:
            for record in event['Records']:
                uploaded = record['s3']
                filename = uploaded['object']['key'].split('/')[-1]

                try:
                    transfer_file(
                        s3_client=s3,
                        bucket=uploaded['bucket']['name'],
                        key=uploaded['object']['key'],
                        sftp_client=sftp,
                        sftp_dest=filename
                    )

                except Exception:
                    print('Could not upload file to SFTP')
                    raise

                else:
                    print('S3 file "{}" uploaded to SFTP "{}" successfully'.format(
                        uploaded['object']['key'], sftp_dir
                    ))


def connect_to_sftp(hostname, port, username, password):
    transport = paramiko.Transport((hostname, port))
    transport.connect(
        username=username,
        password=password
    )
    sftp = paramiko.SFTPClient.from_transport(transport)

    return sftp, transport


def transfer_file(s3_client, bucket, key, sftp_client, sftp_dest):
    """
    Download file from S3 and upload to SFTP
    """

    file_parts_name = sftp_dest.split("_")
    toggle = 1 - int(file_parts_name[-2])
    file_parts_name[-2] = str(toggle)
    old_file = "_".join(file_parts_name)
    try:
        sftp_client.remove(old_file)
    except IOError as e:
        pass
    with sftp_client.file(sftp_dest, 'w') as sftp_file:
        s3_client.download_fileobj(
            Bucket=bucket,
            Key=key,
            Fileobj=sftp_file
        )


def get_config(env, event):
    """
    Get config based on the environment
    """

    url_string = 'http://{0}/entities/pilot/{1}/configs/sftp_pull?access_token={2}'
    base_url = get_base_url(env)
    token = OauthTokenManager.get_access_token()
    pilot = get_pilot(event)
    url = url_string.format(base_url, pilot, token)
    response = requests.get(url)
    payload = response.json()['sftp_pull']
    sftp_pull_config = json.loads(payload)['kvs']
    config = {}
    for sub_config in sftp_pull_config:
        config[sub_config['key']] = sub_config['val']
    return config


def get_pilot(event):
    """
    Get Utility ID from the key
    """
    key = event['Records'][0]['s3']['object']['key']
    pilot = key.split('/')[0]
    return pilot