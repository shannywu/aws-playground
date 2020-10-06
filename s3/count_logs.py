import boto3
import json

s3_client = boto3.client('s3')


def count_number_of_logs(bucket: str, key_list: list):
    """
    Count number of logs from a list of s3 keys.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.select_object_content
    """
    sum_count = 0
    for key in key_list:
        content = s3_client.select_object_content(
            Bucket=bucket,
            Key=key,
            ExpressionType='SQL',
            Expression="select count(*) from s3object s",
            InputSerialization = {  # Should be changed according to input data type
                'CompressionType': 'GZIP',
                'JSON': {'Type': 'LINES',}
            },
            OutputSerialization = {'JSON': {}}
        )

        for event in content['Payload']:
            if 'Records' in event:
                records = event['Records']['Payload'].decode('utf-8')
                sum_count += int(json.loads(records)['_1'])
    return sum_count


if __name__ == '__main__':
    bucket = 'my-s3-bucket'
    obj_list = ['prefix/to/key/data1.gz', 'prefix/to/key/data2.gz']
    count_total = count_number_of_logs(bucket, obj_list)
    print(count_total)
