import boto3
import botocore

BUCKET_NAME = 'viesgo-dev' # replace with your bucket name
KEY = 'temp/file.sh' # replace with your object key

s3 = boto3.resource('s3')

try:
    s3_resource = s3.Bucket(BUCKET_NAME)
    s3_resource.download_file(KEY, 'file_downloaded.sh')

except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise


def get_last_modified_by_s3api_command(bucket_name, archive_dir):
    src = S3 + bucket_name
    # aws --profile=eu s3api list-objects-v2 --bucket "viesgo-prod-eu" --prefix "archive/FIRST" |jq  -c ".[] | max_by(.LastModified)|.LastModified"
    command = ["aws", "s3api", "list-objects-v2", "--bucket", "\"" + bucket_name + "\"", "--prefix",
               "\"" + archive_dir + "\"", "| jq  -c \".[] | max_by(.LastModified)|.LastModified\""]
    try:
        last_modified = (subprocess.check_output(" ".join(command), stderr=subprocess.STDOUT, shell=True)).decode(
            "utf-8").strip()
        last_modified_datetime = datetime.strptime(last_modified.replace('\"', ''), '%Y-%m-%dT%H:%M:%S.000Z')
    except:
        raise
    print("last modified time with prefix "+archive_dir+" is "+str(last_modified_datetime));
    return last_modified_datetime