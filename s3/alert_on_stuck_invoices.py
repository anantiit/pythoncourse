import os
import boto3
import time

S3 = "s3://"

BUCKET_NAME = os.environ.get('BUCKET_NAME')
FILE_PREFIX = os.environ.get('FILE_PREFIX')
ALLOWED_TIME_TO_START_IN_SEC = os.environ.get('ALLOWED_TIME_TO_START_IN_SEC')
PILOT_ID = os.environ.get('PILOT_ID')
ENV = os.environ.get('ENV')
METRIC_NAME = os.environ.get('METRIC_NAME')
NAME_SPACE = os.environ.get('NAME_SPACE')
FILE_PREFIX = "RAW"
METRIC={
            'Namespace': NAME_SPACE,
            'MetricName': METRIC_NAME,
            'Dimensions': [
                {
                    'PilotID': PILOT_ID,
                    'ENV': ENV
                }
            ]

        }
def lambda_handler(event, context):


def fileExists(bucket,prefix):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket)
    objs = list(bucket.objects.filter(Prefix=prefix))
    if len(objs) > 0:
        return True
    return False


def get_last_modified(s3_client, bucket, file_prefix):
    """Get a list of all keys in an S3 bucket."""
    s3_paginator = s3_client.get_paginator('list_objects')
    s3_iterator = s3_paginator.paginate(Bucket=bucket, Prefix=file_prefix)
    try:
        last_modified = None
        last_modified_key = None
        last_modified_obj = None
        for page in s3_iterator:
            objs = page['Contents']
            if len(objs) > 0:
                last_modified_obj_in_curr_page = max(objs, key=lambda k: k['LastModified'])
                last_modified_in_curr_page = last_modified_obj_in_curr_page.get('LastModified')
                if last_modified is None or last_modified < last_modified_in_curr_page:
                    last_modified = last_modified_in_curr_page
                    last_modified_key = last_modified_obj_in_curr_page.get('Key')
                    last_modified_obj = last_modified_obj_in_curr_page
    except KeyError as ke:
        print("The file with prefix " + file_prefix + " is not present. Skipping.." + str(ke))
        return
    except Exception as e:
        raise e
    print("last modified time with prefix " + file_prefix + " is " + str(last_modified) + " and the file name is " + last_modified_key)
    return last_modified_obj

def push_metric(last_modified_obj_time,last_modified_obj_key):
    print("Raising alert...")
    curr_time = int(time.time())
    cloudwatch = boto3.resource('cloudwatch')
    cloudwatch.put_metric_data(METRIC['Namespace'], [{
        'MetricName': METRIC['MetricName'],
        'Dimensions': METRIC['Dimensions'],
        'Timestamp': curr_time,
        'Value': 1
    }])



lambda_handler(None,None)
