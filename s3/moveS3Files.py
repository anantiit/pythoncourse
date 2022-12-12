import os
import boto3
from datetime import datetime
from datetime import timedelta
import time
from dateutil.tz import tzutc

S3 = "s3://"

ARCHIVE_DIR = os.environ.get('ARCHIVE_DIR')  # ingester client after processing it archives files into this
DEST_DIR = os.environ.get('DEST_DIR')  # Ingester client consumes from this prefix
BUCKET_NAME = os.environ.get('BUCKET_NAME')
FIRST_PREFIX = os.environ.get('FIRST_PREFIX')
SECOND_PREFIX = os.environ.get('SECOND_PREFIX')
ERROR_DIR = os.environ.get('ERROR_DIR')
ALLOWED_TIME_TO_START_IN_SEC = os.environ.get('ALLOWED_TIME_TO_START_IN_SEC')
PILOT_ID = os.environ.get('PILOT_ID')
ENV = os.environ.get('ENV')
METRIC_NAME = os.environ.get('METRIC_NAME')
NAME_SPACE = os.environ.get('NAME_SPACE')

print("Environment variables configured : "+" ARCHIVE_DIR:" +ARCHIVE_DIR+" DEST_DIR:" +DEST_DIR+" BUCKET_NAME:" +BUCKET_NAME+" FIRST_PREFIX:" +FIRST_PREFIX+" SECOND_PREFIX:" +SECOND_PREFIX+" ERROR_DIR:" +ERROR_DIR+" ALLOWED_TIME_TO_START_IN_SEC:" +ALLOWED_TIME_TO_START_IN_SEC+" PILOT_ID:" +PILOT_ID+" ENV:" +ENV+" METRIC_NAME:" +METRIC_NAME+" NAME_SPACE:" +NAME_SPACE)

METRIC={
            'Namespace': NAME_SPACE,
            'MetricName': METRIC_NAME,
            'Dimensions': [
                {
                    'Name': 'PilotID',
                    'Value': PILOT_ID
                },
                {
                    'Name': 'ENV',
                    'Value': ENV
                }
            ]

        }

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    try:
        if fileExists(BUCKET_NAME, FIRST_PREFIX):
            move_all_s3_keys(s3_client, BUCKET_NAME, FIRST_PREFIX, DEST_DIR)
        elif fileExists(BUCKET_NAME, SECOND_PREFIX) and not fileExists(BUCKET_NAME, DEST_DIR + FIRST_PREFIX):
                last_modified_obj = get_last_modified(s3_client, BUCKET_NAME, ARCHIVE_DIR + FIRST_PREFIX)
                last_modified_obj_key = last_modified_obj.get('Key')
                last_modified_file_with_out_ext = last_modified_obj_key.rsplit('/', 1)[1].split('.')[0]
                if(fileExists(BUCKET_NAME, ERROR_DIR+last_modified_file_with_out_ext)):
                    print("Last file with prefix "+FIRST_PREFIX+ " in " + ARCHIVE_DIR + "is there in " + ERROR_DIR + " and the file is "+last_modified_file_with_out_ext)
                    move_all_s3_keys(s3_client, BUCKET_NAME,SECOND_PREFIX,DEST_DIR)
                else:
                    print("Last file with prefix "+FIRST_PREFIX+ " in "+ ARCHIVE_DIR + "is not found in " +ERROR_DIR+". Skipping..")
        else:
            print("No files to consume or Files with prefix "+FIRST_PREFIX+" are in "+DEST_DIR+" directory, which means they are not consumed and moved to "+ARCHIVE_DIR+". Hence not moving "+SECOND_PREFIX+" files to "+DEST_DIR)
        putMetricIfFilesWithSecondPrefixGotStuck(s3_client, SECOND_PREFIX)
    except Exception as e:
        print('Exception occured. Hence Exiting! '+str(e))

def fileExists(bucket,prefix):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket)
    objs = list(bucket.objects.filter(Prefix=prefix))
    if len(objs) > 0:
        return True
    return False

def putMetricIfFilesWithSecondPrefixGotStuck(s3_client,SECOND_PREFIX):
    try:
        if fileExists(BUCKET_NAME, SECOND_PREFIX):
            last_modified_obj = get_last_modified(s3_client, BUCKET_NAME, SECOND_PREFIX)
            if last_modified_obj is not None:
                last_modified_obj_time = last_modified_obj.get('LastModified')
                last_modified_obj_key = last_modified_obj.get('Key')
                current_time = datetime.now(tzutc())
                timediff = current_time - last_modified_obj_time
                seconds_diff = timedelta.total_seconds(timediff)
                allowedTimeToStartInSec = float(ALLOWED_TIME_TO_START_IN_SEC)
                if seconds_diff > allowedTimeToStartInSec:
                    print("Last file with prefix " + last_modified_obj_key + " left unprocessed for " + str(
                        seconds_diff) + " seconds but the delay allowed is: " + str(allowedTimeToStartInSec) +". publishing metric...")
                    push_metric(last_modified_obj_time, last_modified_obj_key)
                else:
                    print("Last file with prefix " + last_modified_obj_key + " left unprocessed for " + str(
                        seconds_diff) + " seconds and the delay allowed is: " + str(allowedTimeToStartInSec) + ". Skipping...")
        else:
            print("No files with prefix " + SECOND_PREFIX + ". Hence not publishing metric...")
    except Exception as e:
        print('Exception occured. Hence Exiting! ' + str(e))

def push_metric(last_modified_obj_time,last_modified_obj_key):
    print("Publishing metric...")
    curr_time = int(time.time())
    cloudwatch_client = boto3.client('cloudwatch')
    cloudwatch_client.put_metric_data(Namespace=METRIC['Namespace'],MetricData= [{
        'MetricName': METRIC['MetricName'],
        'Dimensions': METRIC['Dimensions'],
        'Timestamp': curr_time,
        'Value': 1
    }])


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


def move_all_s3_keys(s3_client, bucket, src_prefix, dest_dir):
    """Get a list of all keys in an S3 bucket."""
    s3_paginator = s3_client.get_paginator('list_objects')
    s3_iterator = s3_paginator.paginate(Bucket=bucket, Prefix=src_prefix)
    try:
        for page in s3_iterator:
            objs = page['Contents']
            if len(objs) > 0:
                for obj in objs:
                    original_file_name= obj['Key']
                    original_file_name_with_out_prefix = original_file_name.split("/")[-1]
                    new_file_name = dest_dir + "{}".format(original_file_name_with_out_prefix)
                    s3_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': original_file_name}, Key=new_file_name)
                    s3_client.delete_object(Bucket=bucket, Key=original_file_name)
    except KeyError as ke:
        print("The file with prefix " + src_prefix + " is not present. Skipping.." + str(ke))
        return
    except Exception as e:
        raise e
    print("Moved files with prefix " + src_prefix + " to destination " + dest_dir + " sucessfully.")

#lambda_handler(None,None)
