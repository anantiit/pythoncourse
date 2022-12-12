import boto3
s3 = boto3.client("s3")
# list_objects_v2() give more info

def get_s3_keys(bucket):
    """Get a list of keys in an S3 bucket."""
    keys = []
    resp = s3.list_objects_v2(Bucket=bucket, Prefix="archive/",Delimiter="|")
    for obj in resp['Contents']:
        keys.append(obj['Key'])
    return keys

print(get_s3_keys("viesgo-dev"))
