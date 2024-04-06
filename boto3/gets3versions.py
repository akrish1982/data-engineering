import boto3

AWS_REGION = 'us-east-1'
AWS_PROFILE = 'default'

session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
s3_client = session.client('s3')


# Get the list of object versions
versions = s3_client.list_objects_versions("mybucket")

key_and_version = [versions['Versions'][0]['Key'], versions['Versions'][0]['VersionId']]