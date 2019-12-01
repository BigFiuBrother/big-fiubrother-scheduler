from big_fiubrother_core.db import (
    Database,
    FrameProcess,
    VideoChunkProcess,
    Frame
)
from big_fiubrother_core import QueueTask


class RemoveCompletedProcesses(QueueTask):

    def __init__(self, configuration, input_queue):
        super().__init__(input_queue)
        self.configuration = configuration

    def init(self):
        self.db = Database(self.configuration)

    def execute_with(self, video_chunk_id):
        frame_ids = (
            self.db.session
            .query(Frame.id)
            .filter(Frame.video_chunk_id == video_chunk_id)
            .all()
        )

        for frame_id in frame_ids:
            self.db.delete(
                FrameProcess,
                FrameProcess.frame_id == frame_id)

        self.db.delete(
            VideoChunkProcess,
            VideoChunkProcess.video_chunk_id == video_chunk_id)

    def close(self):
        self.db.close()
