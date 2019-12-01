from big_fiubrother_core.db import (
    Database,
    FrameProcess,
    VideoChunkProcess,
    Frame
)


class ProcessScheduler:

    def __init__(self, configuration):
        self.db = Database(configuration)
        self.cache = {}

    def update_frame_process(self, frame_id):
        with self.db.transaction():
            (
                self.db.session.query()
                .filter(FrameProcess.frame_id == frame_id)
                .update({
                    'processed_faces_count': (
                        FrameProcess.processed_faces_count + 1)
                })
            )

            processed_frame = self.db.query(FrameProcess).get(frame_id)

        if processed_frame.is_completed:
            video_chunk_id = video_chunk_id_for(frame_id)
            return self.update_video_chunk_process(video_chunk_id)
        else:
            return None

    def update_video_chunk_process(self, video_chunk_id):
        with self.db.transaction():
            (
                self.db.session
                .query()
                .filter(VideoChunkProcess.video_chunk_id == video_chunk_id)
                .update({
                    'processed_frames_count': (
                        VideoChunkProcess.processed_frames_count + 1)
                })
            )

            processed_video_chunk = self.db.query(
                VideoChunkProcess).get(video_chunk_id)

        if processed_video_chunk.is_completed:
            return video_chunk_id
        else:
            return None

    def video_chunk_id_for(frame_id):
        if frame_id not in self.cache:
            self.cache[frame_id] = self.db.query(
                Frame.video_chunk_id).get(frame_id)

        return self.cache[frame_id]

    def close(self):
        self.db.close()