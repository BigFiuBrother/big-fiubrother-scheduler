from big_fiubrother_core.db import (
    Database,
    ProcessedFrame,
    ProcessedVideoChunk,
    Frame
)


class ProcessScheduler:

    def __init__(configuration):
        self.db = Database(configuration['db'])
        self.cache = {}

    def update_frame_process(self, frame_id):
        with self.db.transaction():
            self.db.session
            .query()
            .filter(ProcessedFrame.frame_id == frame_id)
            .update({'processed_faces_count': (ProcessedFrame.processed_faces_count + 1)})

            processed_frame = self.db.query(ProcessedFrame).get(frame_id)

        if processed_frame.is_completed:
            video_chunk_id = self.db.query(Frame.video_chunk_id).get(frame_id)
            return self.update_video_chunk_process(video_chunk_id)
        else:
            return None

    def update_video_chunk_process(self, video_chunk_id):
        with self.db.transaction():
            self.db.session
            .query()
            .filter(ProcessedVideoChunk.video_chunk_id == video_chunk_id)
            .update({'processed_frames_count': (ProcessedVideoChunk.processed_frames_count + 1)})

            processed_video_chunk = self.db.query(ProcessedVideoChunk).get(video_chunk_id)

        if processed_video_chunk.is_completed:
            return video_chunk_id
        else:
            return None

