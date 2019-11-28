from . import ProcessScheduler


class ScheduleCompletedProcesses(QueueTask):

    def __init__(self, configuration, input_queue, output_queue):
        super().__init__(input_queue)
        self.output_queue = output_queue
        self.configuration = configuration

    def init(self):
        self.scheduler = ProcessScheduler(self.configuration)

    def execute_with(self, message):
        if message.type == 'ProcessedFaceMessage':
            video_chunk_id = self.scheduler.update_frame_process(
                message.frame_id)
        elif message.type == 'ProcessedFrameMessage':
            video_chunk_id = self.scheduler.update_video_chunk_process(
                message.video_chunk_id)

        if video_chunk_id:
            analyzed_video_chunk_message = AnalyzedVideoChunkMessage(
                video_chunk_id)
            self.output_queue.put(analyzed_video_chunk_message)

    def close(self):
        self.scheduler.close()
