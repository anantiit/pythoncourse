from datetime import datetime
from datetime import timedelta
import boto3
import subprocess

S3 = "s3://"

s3 = boto3.client("s3")
s3_paginator = s3.get_paginator('list_objects')
s3_iterator = s3_paginator.paginate(Bucket='viesgo-dev',Prefix='archive/RAW')
bucket_object_list = []
page = None
for page in s3_iterator:
    pass
if "Contents" in page:
    for key in page[ "Contents" ]:
        keyString = key[ "Key" ]
        bucket_object_list.append(keyString)
print(len(bucket_object_list))

s3 = boto3.resource('s3')
bucket = s3.Bucket('viesgo-dev')
count=0
for key in bucket.objects.filter(Prefix='archive/RAW').all():
    print(key.key)
    count+=1

print(count)

s3 = boto3.resource('s3')
bucket = s3.Bucket('viesgo-dev')
count=0
for key in bucket.objects.filter(Prefix='RAW').all():
        print(key.key)
        count+=1

print(count)

s3 = boto3.resource('s3')
bucket = s3.Bucket('viesgo-dev')
objs = list(bucket.objects.filter(Prefix='RAW'))
if len(objs) > 0:
    print("Exists!"+objs[0].key)
else:
    print("Doesn't exist")
bucket_name="viesgo-dev"
archive_dir = '/archive/'
client = boto3.Session.client();
client = boto3.Session.client(client, service_name = "s3")

paginator = client.get_paginator( "list_objects" )
page_iterator = paginator.paginate( Bucket = "viesgo-dev", Prefix = "archive")






def get_last_modified_by_s3_command(bucket_name, archive_dir):
    src = S3 + bucket_name
    # aws --profile=eu s3api list-objects-v2 --bucket "viesgo-prod-eu" --prefix "archive/RAW" |jq  -c ".[] | max_by(.LastModified)|.LastModified"
    command = ["aws", "s3", "ls",  src + archive_dir +  "| awk '{print $1\" \"$2}' | sort -k1 -k2 | tail -1"]
    try:
        last_modified = (subprocess.check_output(" ".join(command), stderr=subprocess.STDOUT, shell=True)).decode(
            "utf-8").strip()
        print(last_modified)
        last_modified_datetime = datetime.strptime(last_modified, '%Y-%m-%d %H:%M:%S')
    except:
        raise
    return last_modified_datetime

def get_last_modified(S3, bucket_name, archive_dir):
    src = S3 + bucket_name
    # aws --profile=eu s3api list-objects-v2 --bucket "viesgo-prod-eu" --prefix "archive/RAW" |jq  -c ".[] | max_by(.LastModified)|.LastModified"
    command = ["aws", "s3api", "list-objects-v2", "--bucket", "\"" + bucket_name + "\"", "--prefix",
               "\"" + archive_dir + "\"", "| jq  -c \".[] | max_by(.LastModified)|.LastModified\""]
    try:
        last_modified = (subprocess.check_output(" ".join(command), stderr=subprocess.STDOUT, shell=True)).decode(
            "utf-8").strip()
    except:
        raise
    return last_modified.replace('\"', '')

get_last_modified_by_s3_command(bucket_name, archive_dir)

last_modified1 = datetime.strptime("2018-10-13T06:30:13.000Z",'%Y-%m-%dT%H:%M:%S.000Z')
current_time= datetime.now()
timediff = current_time-last_modified1
if timedelta.total_seconds(timediff)>3600:
    print(timedelta.total_seconds(timediff))

print(subprocess.check_output("echo hello world", shell=True).decode("utf-8").strip())

a = '"sajdkasjdsak" "asdasdasds"'

a1=a.replace('"', '')
print(a1)

# or, if they only occur at start and finish
a.strip('\"')

print(a)
