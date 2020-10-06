import boto3
import concurrent.futures
import multiprocessing
import threading

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def list_all_objects(bucket: str):
    """
    List all objects in the given bucket.

    :param bucket: S3 bucket name
    """
    s3_bucket = s3_resource.Bucket(bucket)

    return [obj.key for obj in s3_bucket.objects.all()]


def list_all_objects_with_paginator(bucket: str, prefix='/', delimiter='/'):
    """
    List all objects with paginator.
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html

    :param bucket: S3 bucket name

    :param prefix: S3 prefix that used to filter the paginated results

    :param delimiter: Character you use to group keys
    """
    paginator = s3_client.get_paginator('list_objects_v2')
    objects = []
    prefixes = []

    for resp in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter):
        objects.extend(resp.get('Contents', []))
        prefixes.extend(resp.get('CommonPrefixes', []))

    return {'Contents': objects, 'CommonPrefixes': prefixes}


def list_objects_parallel(bucket: str, prefix='/', delimiter='/', parallelism=None):
    """
    List all objects in parallel way with multiprocessing.
    https://docs.python.org/3/library/multiprocessing.html
    """
    objects = []
    tasks = set()

    if not parallelism:
        parallelism = multiprocessing.cpu_count() * 10

    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as tpe:
        tasks.add(tpe.submit(
            list_all_objects_with_paginator,
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter))

        while tasks:
            done, _ = concurrent.futures.wait(tasks, return_when='FIRST_COMPLETED')
            for future in done:
                res = future.result()
                objects.extend(res['Contents'])

                for cprefix in res['CommonPrefixes']:
                    tasks.add(
                        tpe.submit(
                            list_all_objects_with_paginator,
                            bucket=bucket,
                            prefix=cprefix['Prefix'],
                            delimiter=delimiter))

            tasks = tasks - done

    return {'Contents': objects}


if __name__ == '__main__':
    bucket = 'my-s3-bucket'

    # list all objects
    print(list_all_objects(bucket))

    # list all objects with paginator
    print(list_all_objects_with_paginator(bucket, 'prefix', ''))

    # list all objects in parallel way
    print(list_objects_parallel(bucket, 'prefix', ''))
