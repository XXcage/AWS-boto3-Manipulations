import boto3
import uuid

def create_bucket_name(bucket_prefix):
    """
    generate a uniue name for a bucket.

    unique string is generated and added to prefix.
    Parameters:
        bucket_prefix (str): name provided by user.
    Returns:
        str: Unique name with prefix added to it.
    """
    return ''.join([bucket_prefix, str(uuid.uuid4())])
  
def create_bucket(bucket_prefix, s3_connection):
    """
    Create s3 bucket, return its name and metadata.

    create an s3 bucket, "bucket_prefix" is added to a generated unique str

    Parameters:
        bucket_prefix (str): name provided by user.
        s3_connection : client/resource interface.
    Returns:
        bucket_name (str): prefix+generated name of the bucket.
        bucket_response : bucket metadata.
    """
    session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = create_bucket_name(bucket_prefix)
    bucket_response = s3_connection.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': current_region})
    print(bucket_name, current_region)
    return bucket_name, bucket_response

def create_temp_file(size, file_name, file_content):
    """
    takes 'size' * 'file content' and outputs that in 'random_file_name' variable
    adding random generated string to the name.
    """
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name

def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    """
    copies a file named "file_name" from bucket "bucket_form_name" to "bucket_to_name"
    """
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    s3_resource.Object(bucket_to_name, file_name).copy(copy_source)

def enable_bucket_versioning(bucket_name):
    """
    enables bucket versioning
    """
    bkt_versioning = s3_resource.BucketVersioning(bucket_name)
    bkt_versioning.enable()
    print(bkt_versioning.status)

def delete_all_objects(bucket_name):
    """
    deletes all objects from a bucket named "bucket_name"
    """
    res = []
    bucket=s3_resource.Bucket(bucket_name)
    for obj_version in bucket.object_versions.all():
        res.append({'Key': obj_version.object_key,
                    'VersionId': obj_version.id})
    print(res)
    bucket.delete_objects(Delete={'Objects': res})

if __name__ == "__main__":

    
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    # create first bucket using client connection type
    try:
        first_bucket_name, first_response = create_bucket(
            bucket_prefix='firstpythonbucket', 
            s3_connection=s3_resource.meta.client)
    except Exception as e:
        print("exception:", type(e).__name__)

    # create second bucket using resource connection type

    try:
        second_bucket_name, second_response = create_bucket(      
            bucket_prefix='secondpythonbucket', s3_connection=s3_resource)
    except Exception as e:
        print("exception:", type(e).__name__)

    first_file_name = create_temp_file(300, 'firstfile.txt', 'f')   

    # upload the file to the first bucket

    first_bucket = s3_resource.Bucket(name=first_bucket_name)
    first_object = s3_resource.Object(
        bucket_name=first_bucket_name, key=first_file_name)
    try:
        first_object.upload_file(first_file_name)
    except Exception as e:
        print("exception:", type(e).__name__)

    # download the first file that was created with create_temp_file

    try:
        s3_resource.Object(first_bucket_name, first_file_name).download_file(
            f'/tmp/{first_file_name}')
    except Exception as e:
        print("exception:", type(e).__name__)

    #copy the file from first bucket to second bucket and then delete it
    copy_to_bucket(first_bucket_name, second_bucket_name, first_file_name)
    s3_resource.Object(second_bucket_name, first_file_name).delete()

    #craete second file and set access to public
    second_file_name = create_temp_file(400, 'secondfile.txt', 's')
    second_object = s3_resource.Object(first_bucket.name, second_file_name)
    second_object.upload_file(second_file_name, ExtraArgs={
                            'ACL': 'public-read'})

    # put acl into var and print it
    second_object_acl = second_object.Acl()
    print(second_object_acl.grants)                  

    #set access to private
    response = second_object_acl.put(ACL='private')
    print(second_object_acl.grants)

    #third file > first bucket > encryption
    third_file_name = create_temp_file(300, 'thirdfile.txt', 't')
    third_object = s3_resource.Object(first_bucket_name, third_file_name)
    third_object.upload_file(third_file_name, ExtraArgs={
                            'ServerSideEncryption': 'AES256'})
    print(third_object.server_side_encryption)

    #third file> first bucket > storage class definition
    third_object.upload_file(third_file_name, ExtraArgs={
                            'ServerSideEncryption': 'AES256', 
                            'StorageClass': 'STANDARD_IA'})
    third_object.reload()
    print(third_object.storage_class)

    # + versioning to first bucket 
    enable_bucket_versioning(first_bucket_name)

    #second file > first bucket > print version ID of the first file
    s3_resource.Object(first_bucket_name, second_file_name).upload_file(
        second_file_name)
    print(s3_resource.Object(first_bucket_name, first_file_name).version_id)

    #iterate thru all buckets and print their names via resource
    for bucket in s3_resource.buckets.all():
        print(bucket.name)

    #iterate thru all buckets and print their names using client
    for bucket_dict in s3_resource.meta.client.list_buckets().get('Buckets'):
        print(bucket_dict['Name'])

    #print keys of all the objects in the first bucket
    for obj in first_bucket.objects.all():
        print(obj.key)

    #print keys,storage classes, date of modification, version id , metadata
    #of all objects in first bucket
    for obj in first_bucket.objects.all():
        subsrc = obj.Object()
        print(obj.key, obj.storage_class, obj.last_modified,
            subsrc.version_id, subsrc.metadata)

    #delete all objects in first bucket
    try:
        delete_all_objects(first_bucket_name)
    except Exception as e:
        print("exception:", type(e).__name__)

    #first file > second bucket, then delete all files form second bucket
    s3_resource.Object(second_bucket_name, first_file_name).upload_file(
        first_file_name)
    try:
        delete_all_objects(second_bucket_name)
    except Exception as e:
        print("exception:", type(e).__name__)

    #delete buckets first and second
    try:
        s3_resource.Bucket(first_bucket_name).delete()
        s3_resource.meta.client.delete_bucket(Bucket=second_bucket_name)
    except Exception as e:
        print("exception:", type(e).__name__)
