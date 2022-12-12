import boto3
users=open("/Users/appalanaiduabotula/temp/users.txt","r+")
if(users.readable()):
    for line in users.readlines():
        print(line)
    users.write("\nNaidu loves Bharati")
users.close()

boto3.c