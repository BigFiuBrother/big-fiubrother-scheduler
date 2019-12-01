from . import ProcessScheduler
from big_fiubrother_core import QueueTask
from big_fiubrother_core.messages import AnalyzedVideoChunkMessage


class ScheduleCompletedProcesses(QueueTask):

    def __init__(
            self, configuration, input_queue, publisher_queue, removal_queue):
        super().__init__(input_queue)
        self.publisher_queue = publisher_queue
        self.removal_queue = removal_queue
        self.configuration = configuration

    def init(self):
        self.scheduler = ProcessScheduler(self.configuration)

    def execute_with(self, message):
        video_chunk_id = self._process_message(message)

        if video_chunk_id:
            analyzed_video_chunk_message = AnalyzedVideoChunkMessage(
                video_chunk_id)

            self.publisher_queue.put(analyzed_video_chunk_message)
            self.removal_queue.put(video_chunk_id)

    def close(self):
        self.scheduler.close()

    def _process_message(self, message):
        if message.type() == 'ProcessedFaceMessage':
            return self.scheduler.update_frame_process(
                message.frame_id)
        elif message.type() == 'ProcessedFrameMessage':
            return self.scheduler.update_video_chunk_process(
                message.video_chunk_id)

        return None
