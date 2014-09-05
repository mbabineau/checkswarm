#!/usr/bin/env python
import re
import socket
import subprocess
import sys
import threading
import time

try:
    from mesos.native import MesosExecutorDriver, MesosSchedulerDriver
    from mesos.interface import Executor, Scheduler
    from mesos.interface import mesos_pb2
except ImportError:
    from mesos import Executor, MesosExecutorDriver, MesosSchedulerDriver, Scheduler
    import mesos_pb2


class CheckExecutor(Executor):
    # Replaces invalid characters for use in Graphite
    CARBON_REPLACEMENT_CHARACTER = '_'

    def __init__(self, carbon_host, carbon_port=2003):
        self.carbon_host = carbon_host
        self.carbon_port = carbon_port
        self.carbon_queue = []

    def _send_to_graphite(self, metric_path, value, ts=None):
        if not ts:
            ts = int(time.time())
        message = '%s %s %s\n' % (metric_path, value, ts)
        sock = socket.socket()
        sock.connect((self.carbon_host, self.carbon_port))
        sock.sendall(message)
        sock.close()

    def _extract_perfdata(self, raw_perfdata):
        """Parses Nagios check performance data and returns a list of (label, value) tuples"""
        # Adapted from https://github.com/shawn-sterling/graphios/blob/master/graphios.py
        matches = re.finditer(r'(?P<perfdata>(?P<label>.*?)=(?P<value>[-0-9\.]+)\S*\s?)', raw_perfdata)
        parsed_perfdata = [match.groupdict() for match in matches]
        return [
            (re.sub(r'[\s\.:\\/]', self.CARBON_REPLACEMENT_CHARACTER, p['label']), p['value'])
            for p in parsed_perfdata
        ]

    def _handle_result(self, check_command, args, return_code, output):
        print '%s returned with code [%s] and message: %s' % (check_command, return_code, output)

        raw_perfdata = output.split('|')[1]
        perfdata = self._extract_perfdata(raw_perfdata)

        host = re.sub(r'[\s\.:\\]', self.CARBON_REPLACEMENT_CHARACTER, args[args.index('-H')+1]) if '-H' in args else 'local'

        metric_prefix = '.'.join([check_command, host])
        if check_command == 'check_http':
            url = re.sub(r'[\s\.:\\\/\()]', self.CARBON_REPLACEMENT_CHARACTER, args[args.index('-u')+1]) if '-u' in args else '_'
            metric_prefix = '.'.join([metric_prefix, url])

        # Send check result and perf data to Graphite
        self._send_to_graphite('.'.join([metric_prefix, '_status']), return_code)
        for label, value in perfdata:
            self._send_to_graphite('.'.join([metric_prefix, label]), value)

    def _parse_task_data(self, task):
        args = task.data.split(' ')
        check_command = args.pop(0)
        return check_command, args

    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        print 'CheckExecutor registered'

    def reregistered(self, driver, slaveInfo):
        print 'CheckExecutor reregistered'

    def disconnected(self, driver):
        print 'CheckExecutor disconnected'

    def launchTask(self, driver, task):
        def run_task():
            print 'Running check task %s' % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            check_command, args = self._parse_task_data(task)

            try:
                output = subprocess.check_output(['docker', 'run', '--rm', 'thefactory/nagios-plugins', check_command]+args)
                return_code = 0
            except subprocess.CalledProcessError, e:
                output = e.output
                return_code = e.returncode

            self._handle_result(check_command, args, return_code, output)

            print 'Sending status update for task %s' % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            update.message = output
            driver.sendStatusUpdate(update)
            print 'Sent status update for task %s' % task.task_id.value
            return

        thread = threading.Thread(target=run_task)
        thread.start()

    def killTask(self, driver, taskId):
        self.shutdown(self, driver)

    def frameworkMessage(self, driver, message):
        pass

    def shutdown(self, driver):
        print "Shutting down"
        sys.exit(0)

    def error(self, error, message):
        pass

if __name__ == '__main__':
    print 'Starting CheckExecutor'
    carbon_host = sys.argv[1]
    driver = MesosExecutorDriver(CheckExecutor(carbon_host))
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)