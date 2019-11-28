#!/usr/bin/env python3

from queue import Queue
from big_fiubrother_core import (
    SignalHandler,
    StoppableThread,
    PublishToRabbitMQ,
    ConsumeFromRabbitMQ,
    setup
)
from big_fiubrother_scheduler import (
    ScheduleCompletedProcesses
)


if __name__ == "__main__":
    configuration = setup('Big Fiubrother Scheduler Application')

    print('[*] Configuring big-fiubrother-scheduler')

    consumer_to_scheduler_queue = Queue()
    scheduler_to_publisher_queue = Queue()

    consumer = StoppableThread(
        ConsumeFromRabbitMQ(configuration=configuration['consumer'],
                            output_queue=consumer_to_scheduler_queue))

    video_sampler = StoppableThread(
        ScheduleCompletedProcesses(configuration=configuration,
                                   input_queue=consumer_to_scheduler_queue,
                                   output_queue=scheduler_to_publisher_queue))

    publisher = StoppableThread(
        PublishToRabbitMQ(configuration=configuration['publisher'],
                          input_queue=scheduler_to_publisher_queue))

    signal_handler = SignalHandler(callback=consumer.stop)

    print('[*] Configuration finished. Starting big-fiubrother-scheduler!')

    publisher.start()
    scheduler.start()
    consumer.run()

    # Signal Handled STOP
    scheduler.stop()
    publisher.stop()

    scheduler.wait()
    publisher.wait()

    print('[*] big-fiubrother-scheduler stopped!')
