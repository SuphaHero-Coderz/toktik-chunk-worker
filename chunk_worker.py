import os
import logging
import json
import uuid
import redis
import ffmpeg
import boto3
import botocore
from moviepy.editor import VideoFileClip

# Redis Credentials
LOG = logging
REDIS_QUEUE_LOCATION = os.getenv('REDIS_QUEUE', 'localhost')
# Queue_name which is identical to the one in chunk() in the backend, this worker listen to this queue
QUEUE_NAME = 'queue:chunk'

INSTANCE_NAME = uuid.uuid4().hex

LOG.basicConfig(
    level=LOG.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

s3 = boto3.client('s3', 
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
)


# function to watch the workqueue and fetch work when exist
def watch_queue(redis_conn, queue_name, callback_func, timeout=30):
    active = True

    while active:
        # Fetch a json-encoded task using a blocking (left) pop
        packed = redis_conn.blpop([queue_name], timeout=timeout)

        if not packed:
            # if nothing is returned, poll a again
            continue

        _, packed_task = packed

        # If it's treated to a poison pill, quit the loop
        if packed_task == b'DIE':
            active = False
        else:
            task = None
            try:
                task = json.loads(packed_task)
            except Exception:
                LOG.exception('json.loads failed')
                data = { "status" : -1, "message" : "An error occurred" }
                redis_conn.publish("chunk", json.dumps(data))
            if task:
                callback_func(task["object_key"])
                data = { "status" : 1, "message" : "Successfully chunked video" }
                redis_conn.publish("chunk", json.dumps(task))

def download_video(object_key: str):
    try:
        LOG.info("Downloading file from S3 for chunking")
        s3.download_file(os.getenv("BUCKET_NAME"), f"{object_key}/encoded.mp4", "encoded.mp4")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            LOG.error("ERROR: file was not found on S3")
        else:
            LOG.error("ERROR: file download")
            raise

def delete_video(object_key: str):
    LOG.info("Deleting original video")
    response = s3.delete_object(Bucket=os.getenv("BUCKET_NAME"), Key=object_key)
    LOG.info(response)

def upload_chunks(object_key: str):
    print("===")
    print(os.listdir("."))
    print(os.listdir("./chunks"))
    print("===")
    LOG.info("Uploading chunks video")
    try:
        for file in os.listdir("./chunks"):
            s3.upload_file(f"./chunks/{file}", os.getenv("BUCKET_NAME"), f"{object_key}/chunks/{file}")
            LOG.info("Successfully uploaded chunk {file}")
    except botocore.exceptions.ClientError as e:
        LOG.error(e)

def chunk_video():
    clip = VideoFileClip("encoded.mp4").filename.split('.')[0]
    input_stream = ffmpeg.input("encoded.mp4", f='mp4')
    output_stream = ffmpeg.output(input_stream, f'./chunks/{clip}.m3u8', format='hls', start_number=0, hls_time=10, hls_list_size=0)
    ffmpeg.run(output_stream)

def cleanup():
    try:
        os.remove("./encoded.mp4")
        for file in os.listdir("./chunks"):
            file_path = f"./chunks/{file}"
            if os.path.isfile(file_path):
                os.remove(file_path)
        LOG.info("All files deleted successfully.")
    except OSError:
        LOG.error("Error occurred while deleting files.")

def execute_chunk(object_key: str):
    #print("execute chunk")
    download_video(object_key)
    chunk_video()
    upload_chunks(object_key)
    cleanup()

def main():
    LOG.info('Starting a worker...')
    LOG.info('Unique name: %s', INSTANCE_NAME)
    host, *port_info = REDIS_QUEUE_LOCATION.split(':')
    port = tuple()
    if port_info:
        port, *_ = port_info
        port = (int(port),)

    named_logging = LOG.getLogger(name=INSTANCE_NAME)
    named_logging.info('Trying to connect to %s [%s]', host, REDIS_QUEUE_LOCATION)
    redis_conn = redis.Redis(host=host, *port)
    watch_queue(
        redis_conn,
        QUEUE_NAME,
        execute_chunk)


if __name__ == '__main__':
    main()
