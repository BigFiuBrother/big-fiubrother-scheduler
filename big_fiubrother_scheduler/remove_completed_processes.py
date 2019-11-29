from big_fiubrother_core.db import (
    Database,
    ProcessedFrame,
    ProcessedVideoChunk,
    Frame
)


class RemoveCompletedProcesses(QueueTask):

    def __init__(self, configuration, input_queue):
        super().__init__(input_queue)
        self.configuration = configuration

    def init(self):
        self.db = Database(configuration)

    def execute_with(self, video_chunk_id):
        frame_ids = (
            self.db.query(Frame.id)
            .filter(Frame.video_chunk_id == video_chunk_id)
            .all
        )

        with self.db.transaction():
            for frame_id in frame_ids:
                self.db.delete(ProcessedFrame(frame_id=frame_id))

            self.db.delete(ProcessedVideoChunk(video_chunk_id=video_chunk_id))

    def close(self):
        self.db.close()
