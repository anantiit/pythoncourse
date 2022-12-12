import boto3
import botocore




def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    s3 = boto3.resource('s3')
    s3_bucket = s3.
    key
    s3_source = ""
    s3_dest = "Incoming"
    file_prefix
    transfer_file(s3_client, bucke    t, key,s3_source, s3_dest,file_prefix)



def get_all_s3_keys(bucket):
    """Get a list of all keys in an S3 bucket."""
    keys = []

    kwargs = {'Bucket': bucket}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys