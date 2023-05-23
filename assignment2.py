import json
from PIL import Image
from io import BytesIO
import os
import boto3

s3 = boto3.client("s3")
bucket = 'test-bucket-lambda-012'
key = 'test.png'

def lambda_handler():
    response = s3.get_object(Bucket=bucket, Key=key)['Body'].read()

    img = Image.open(BytesIO(response))
    rgb_im = img.convert("RGB")
    # ext = os.path.splitext(key)[-1].strip('.')

    jpeg_image = BytesIO()
    rgb_im.save(jpeg_image, 'jpeg')
    jpeg_image.seek(0)
    sent_data = s3.put_object(Bucket=bucket, Key='test.jpeg', Body=jpeg_image)

    if sent_data['ResponseMetadata']['HTTPStatusCode'] != 200:
        return "Failed to upload"
    else:
        return True

print(lambda_handler())

# import boto3
#
# s3 = boto3.client('s3')
#
# s3.upload_file('C:/Users/Tejas/Desktop/python/aws/lambdaFunction.jpeg',Bucket='test-bucket-lambda-012', Key='lambdaFunction.jpeg')


#list of buckets
# buckets = s3.list_buckets()
# buckets = [bucket['Name'] for bucket in buckets['Buckets']]
# print(buckets)
#list of objects
# objects = s3.list_objects(Bucket='test-bucket-021')
# obj = [object['Key'] for object in objects['Contents']]
# print(obj)

#print list of buckets - boto.resource('s3')
# for i in buckets['Buckets']:
#     print(i.name)

#Create bucket
# filename = 'extdata.txt'
# bucket_name = 'my-bucket-987'
# s3.create_bucket(Bucket='my-bucket-987', CreateBucketConfiguration={
#         'LocationConstraint': 'ap-south-1'
#     })

#upload file to bucket
# s3.upload_file("C:/Users/Tejas/Desktop/test/extdata.txt", bucket_name, filename)

#check list of objects in a bucket
# objects = s3.list_objects_v2(Bucket="test-bucket-021")
# fileCount = objects['KeyCount']
# print(fileCount)

#delete a file from bucket
# response = s3.delete_object(Bucket='my-bucket-987', Key='extdata.txt')

#delete bucket
# res = s3.delete_bucket(Bucket='my-bucket-987')

#empty bucket
# s3 = boto3.resource("s3")
#
# buckets = s3.Bucket("test-bucket-021")
# buckets.objects.all().delete()
