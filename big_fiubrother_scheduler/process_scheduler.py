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
        frame_process = self.db.get(FrameProcess, frame_id)
        frame_process.processed_faces_count = FrameProcess.processed_faces_count + 1
        self.db.update()

        if frame_process.is_completed():
            video_chunk_id = self._video_chunk_id_for(frame_process.frame_id)
            return self.update_video_chunk_process(video_chunk_id)
        else:
            return None

    def update_video_chunk_process(self, video_chunk_id):
        video_chunk_process = self.db.get(VideoChunkProcess, video_chunk_id)
        video_chunk_process.processed_frames_count = VideoChunkProcess.processed_frames_count + 1
        self.db.update()

        if video_chunk_process.is_completed():
            return video_chunk_process.video_chunk_id
        else:
            return None

    def _video_chunk_id_for(self, frame_id):
        if frame_id not in self.cache:
            self.cache[frame_id] = self.db.get(Frame, frame_id).video_chunk_id

        return self.cache[frame_id]

    def close(self):
        self.db.close()
