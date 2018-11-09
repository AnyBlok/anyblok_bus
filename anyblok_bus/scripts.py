# This file is a part of the AnyBlok / Bus api project
#
#    Copyright (C) 2018 Jean-Sebastien SUZANNE <jssuzanne@anybox.fr>
#
# This Source Code Form is subject to the terms of the Mozilla Public License,
# v. 2.0. If a copy of the MPL was not distributed with this file,You can
# obtain one at http://mozilla.org/MPL/2.0/.
import os
import signal
import time
from anyblok import load_init_function_from_entry_points
from anyblok.config import Configuration
from anyblok.blok import BlokManager
from anyblok.registry import RegistryManager
from .worker import Worker
from .release import version
from logging import getLogger

logger = getLogger(__name__)


Configuration.add_application_properties(
    'bus', ['logging', 'bus'],
    prog='Bus app for AnyBlok, version %r' % version,
    description='Bus for AnyBlok',
)


def bus_worker_process(worker_id, logging_fd):
    """consume worker to process messages and execute the actor"""
    # TODO preload registries
    db_name = Configuration.get('db_name')
    profile = Configuration.get('bus_profile')
    try:
        logging_pipe = os.fdopen(logging_fd, "w")
        BlokManager.load()
        registry = RegistryManager.get(db_name, loadwithoutmigration=True)
        if registry is None:
            logger.critical("No registry found for %s", db_name)
            return os._exit(4)
        worker = Worker(registry, profile)
        worker.start()
    except ImportError as e:
        logger.critical(e)
        return os._exit(2)
    except ConnectionError as e:
        logger.critical("Broker connection failed. %s", e)
        return os._exit(3)

    def termhandler(signum, frame):
        nonlocal running
        BlokManager.unload()
        if running:
            logger.info("Stopping worker process...")
            running = False
        else:
            logger.warning("Killing worker process...")
            return os._exit(1)

    logger.info("Worker process is ready for action.")
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, termhandler)
    signal.signal(signal.SIGHUP, termhandler)

    running = True
    while running:
        time.sleep(1)

    worker.stop()
    logging_pipe.close()


def anyblok_bus():
    """Run consumer workers process to consume queue

    :param application: name of the application
    :param configuration_groups: list configuration groupe to load
    :param **kwargs: ArgumentParser named arguments
    """
    load_init_function_from_entry_points()
    Configuration.load('bus')

    worker_pipes = []
    worker_processes = []
    for worker_id in range(Configuration.get('bus_processes', 1)):
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid != 0:
            os.close(write_fd)
            worker_pipes.append(os.fdopen(read_fd))
            worker_processes.append(pid)
            continue

        os.close(read_fd)
        return bus_worker_process(worker_id, write_fd)

    def sighandler(signum, frame):
        nonlocal worker_processes
        signum = {
            signal.SIGINT: signal.SIGTERM,
            signal.SIGTERM: signal.SIGTERM,
            signal.SIGHUP: signal.SIGHUP,
        }[signum]

        logger.info("Sending %r to worker processes...", signum.name)
        for pid in worker_processes:
            try:
                os.kill(pid, signum)
            except OSError:
                logger.warning(
                    "Failed to send %r to pid %d.", signum.name, pid)

    retcode = 0
    signal.signal(signal.SIGINT, sighandler)
    signal.signal(signal.SIGTERM, sighandler)
    signal.signal(signal.SIGHUP, sighandler)
    for pid in worker_processes:
        pid, rc = os.waitpid(pid, 0)
        retcode = max(retcode, rc >> 8)

    running = False  # noqa
    for pipe in worker_pipes:
        pipe.close()

    return retcode
