#!/usr/bin/env python
import json
import signal
import socket
import sys
import time
from threading import Thread

try:
    from mesos.native import MesosExecutorDriver, MesosSchedulerDriver
    from mesos.interface import Executor, Scheduler
    from mesos.interface import mesos_pb2
except ImportError:
    from mesos import Executor, MesosExecutorDriver, MesosSchedulerDriver, Scheduler
    import mesos_pb2


TASK_CPUS = 0.1
TASK_MEM = 8
SHUTDOWN_TIMEOUT = 30  # in seconds
LEADING_ZEROS_COUNT = 5  # appended to task ID to facilitate lexicographical order
TASK_ATTEMPTS = 5  # how many times a task is attempted

# Fake queue for POC
CHECK_QUEUE = [
    'check_http -H www.google.com',
    'check_http -H www.docker.com -S',
    'check_http -H mesosphere.io',
    'check_http -H www.amazon.com',
    'check_http -H www.thefactory.com',
    'check_http -H www.github.com',
    'check_http -H en.wikipedia.org -u /wiki/E._Clarke_and_Julia_Arnold_House',
    'check_http -H en.wikipedia.org -u /wiki/Hendel_Brothers,_Sons_and_Company_Hat_Factory',
    'check_http -H en.wikipedia.org -u /wiki/Raedieahkka',
    'check_http -H en.wikipedia.org -u /wiki/Celtic_music_in_Canada',
    'check_http -H en.wikipedia.org -u /wiki/Philip_Bermingham',
    'check_http -H en.wikipedia.org -u /wiki/Fabric_49',
    'check_http -H en.wikipedia.org -u /wiki/Kahrizak,_Pakdasht',
    'check_http -H en.wikipedia.org -u /wiki/WOWO_(AM)',
    'check_load -w 0 -c 3'
]

TASK_STATES = {
    6: 'TASK_STAGING',  # Initial state. Framework status updates should not use.
    0: 'TASK_STARTING',
    1: 'TASK_RUNNING',
    2: 'TASK_FINISHED', # TERMINAL.
    3: 'TASK_FAILED',   # TERMINAL.
    4: 'TASK_KILLED',   # TERMINAL.
    5: 'TASK_LOST'      # TERMINAL.
}


class CheckSwarm(Scheduler):
    def __init__(self, carbon_host, statsd_host, carbon_port=2003, statsd_port=8125):
        print 'checkswarm init'
        self.tasks_created = 0
        self.timers = {}
        self.carbon_host = carbon_host
        self.carbon_port = carbon_port
        self.statsd_host = statsd_host
        self.statsd_port = statsd_port

        self.queue_index = 0 # for fake queue

    def _report_task_timing(self, time_in_ms):
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.sendto(
            ''.join(['checkswarm:', str(time_in_ms), '|ms']).encode('utf-8'),
            (self.statsd_host, self.statsd_port)
        )

    def _calc_max_tasks(self, offer):
        count = 0
        cpus = next(rsc.scalar.value for rsc in offer.resources if rsc.name == 'cpus')
        mem = next(rsc.scalar.value for rsc in offer.resources if rsc.name == 'mem')
        while cpus >= TASK_CPUS and mem >= TASK_MEM:
            count += 1
            cpus -= TASK_CPUS
            mem -= TASK_MEM
        return count

    def _build_task(self, offer, check):
        task = mesos_pb2.TaskInfo()

        tid = self.tasks_created
        self.tasks_created += 1
        task.task_id.value = str(tid).zfill(LEADING_ZEROS_COUNT)
        task.slave_id.value = offer.slave_id.value

        cpus = task.resources.add()
        cpus.name = 'cpus'
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS

        mem = task.resources.add()
        mem.name = 'mem'
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM

        task.name = 'check %s' % task.task_id.value

        container = mesos_pb2.ContainerInfo()
        container.type = mesos_pb2.ContainerInfo.DOCKER
        docker = mesos_pb2.ContainerInfo.DockerInfo()
        docker.image = 'thefactory/nagios-plugins'
        container.docker.CopyFrom(docker)
        task.container.CopyFrom(container)

        task.data = check

        # command = mesos_pb2.CommandInfo()
        # command.shell = False
        # command.arguments.extend(check.split(' '))
        # task.command.CopyFrom(command)

        executor = mesos_pb2.ExecutorInfo()
        # executor.executor_id.value = '-'.join(['check_executor',task.task_id.value])
        executor.executor_id.value = 'check_executor'
        executor.command.value = 'python /tmp/check_executor.py %s' % self.carbon_host
        task.executor.MergeFrom(executor)
        return task

    def registered(self, driver, frameworkId, masterInfo):
        print 'Registered with framework ID [%s]' % frameworkId.value

    def reregistered(self, driver, frameworkId, masterInfo):
        print 'Reregistered with framework ID [%s]' % frameworkId.value

    def disconnected(self, driver, frameworkId, masterInfo):
        print 'Disconnected'

    def resourceOffers(self, driver, offers):
        for offer in offers:
            print 'Got resource offer [%s]' % offer.id.value

            tasks = []
            if CHECK_QUEUE:
                for i in range(self._calc_max_tasks(offer)):
                    tasks.append(self._build_task(offer, CHECK_QUEUE[self.queue_index]))

                    # loop over fake queue:
                    self.queue_index += 1
                    if self.queue_index == len(CHECK_QUEUE): self.queue_index = 0
                    # alternatively, iterate just once:
                    # tasks.append(self._build_task(offer, CHECK_QUEUE.pop()))

            if tasks:
                print 'Accepting offer on [%s]' % offer.hostname
                driver.launchTasks(offer.id, tasks)
                [self.timers.update({ task.task_id.value: time.time()}) for task in tasks]
            else:
                print 'Declining offer on [%s]' % offer.hostname
                driver.declineOffer(offer.id)

    def statusUpdate(self, driver, update):
        state_name = TASK_STATES[update.state]
        print 'Task [%s] is in state [%s] with message: %s' % (update.task_id.value, state_name, update.message)
        if state_name in ['TASK_FINISHED', 'TASK_FAILED', 'TASK_KILLED', 'TASK_LOST']:
            end_time = time.time()
            start_time = self.timers.pop(update.task_id.value, end_time)
            print 'Task %s completed in %s seconds' % (update.task_id.value, end_time-start_time)
            self._report_task_timing(int(1000*(end_time-start_time)))

    def frameworkMessage(self, driver, executorId, slaveId, message):
        o = json.loads(message)
        print 'Got framework message: %s' % o


def shutdown(signal, frame):
    driver.stop()


if __name__ == '__main__':
    framework = mesos_pb2.FrameworkInfo()
    framework.user = '' # Have Mesos fill in the current user.
    framework.name = 'checkswarm'
    framework.failover_timeout = 60

    statsd_host = sys.argv[2]
    carbon_host = statsd_host
    checkswarm = CheckSwarm(carbon_host, statsd_host)

    driver = MesosSchedulerDriver(checkswarm, framework, sys.argv[1])

    # driver.run() blocks; we run it in a separate thread
    def run_driver_async():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)
    framework_thread = Thread(target = run_driver_async, args = ())
    framework_thread.start()

    print '(Listening for Ctrl-C)'
    signal.signal(signal.SIGINT, shutdown)
    while framework_thread.is_alive():
        time.sleep(1)

    print 'Goodbye!'
    sys.exit(0)