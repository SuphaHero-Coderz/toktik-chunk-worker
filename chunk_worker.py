import os
import logging
import json
import uuid
import redis
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
                redis_conn.publish("chunk", "chunk_ok")
            # publish message back to the broker and call chunk function on the task
            if task:
                callback_func(task["name"])
                redis_conn.publish("chunk", "chunk_ok")


# the actual chunking process, using moviepy
def execute_chunk(file_path: str):
    current_duration = VideoFileClip(file_path).duration
    divide_into_count = 5
    single_duration = current_duration / divide_into_count

    # loop and create sub clips (chunking)
    while current_duration > single_duration:
        clip = VideoFileClip(file_path).subclip(current_duration - single_duration, current_duration)
        current_duration -= single_duration
        current_video = f"{current_duration}.mp4"
        clip.to_videofile(current_video, codec="libx264", temp_audiofile='temp-audio.m4a', remove_temp=True,
                          audio_codec='aac')
        print("-----------------###-----------------")
    clip = VideoFileClip(file_path).subclip(0, current_duration)
    current_duration -= single_duration
    current_video = f"0.mp4"
    clip.to_videofile(current_video, codec="libx264", temp_audiofile='temp-audio.m4a', remove_temp=True,
                      audio_codec='aac')
    print("-----------------###-----------------")


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
