from big_fiubrother_core.message_clients.rabbitmq import Publisher
from big_fiubrother_core.messages import (
    ProcessedFaceMessage,
    ProcessedFrameMessage
)
from big_fiubrother_core.db import (
    VideoChunk,
    Frame,
    FrameProcess,
    VideoChunkProcess,
    Database
)


db_configuration = {
    'host': 'localhost',
    'database': 'big_fiubrother_test',
    'username': 'postgres',
    'password': 'password'
}

db = Database(db_configuration)

video_chunk = VideoChunk(camera_id='CAMERA',
                         timestamp=0.0,
                         payload=b'payload')

db.add(video_chunk)

frame_ids = []
for offset in range(2):
    frame = Frame(offset=offset,
                  video_chunk_id=video_chunk.id)

    db.add(frame)
    frame_ids.append(frame.id)


db.add(VideoChunkProcess(video_chunk_id=video_chunk.id,
                         total_frames_count=2))

db.add(FrameProcess(frame_id=frame_ids[0],
                    total_faces_count=2))

# Publish messages for run

publisher_configuration = {
    'host': 'localhost',
    'username': 'fiubrother',
    'password': 'alwayswatching',
    'exchange': 'fiubrother',
    'routing_key': 'scheduler'
}

publisher = Publisher(publisher_configuration)

publisher.publish(ProcessedFrameMessage(video_chunk_id=video_chunk.id))

for i in range(2):
    publisher.publish(ProcessedFaceMessage(frame_id=frame_ids[0]))
