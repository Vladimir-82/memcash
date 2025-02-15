"""Memcache Load module."""

import os
import gzip
import sys
import glob
import logging
import collections
import time
import multiprocessing
from multiprocessing.pool import ThreadPool
from collections import defaultdict
from memcache import Client
from optparse import OptionParser

from appsinstalled_pb2 import UserApps


AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])

NORMAL_ERR_RATE = 0.01
TIMEOUT = 5
MAXATTEMPTS = 5


class ProcessedAppsCounter(object):
    """Class Helper to count errors and processed apps."""

    processed_apps = 0
    errors_apps = 0

    def error(self, count=1):
        """Errors."""
        self.errors_apps += count

    def processed(self, count=1):
        """Processed."""
        self.processed_apps += count

    def get_processed(self):
        """Get processed."""
        return self.processed_apps

    def get_errors_rate(self):
        """Get error rate."""
        try:
            err_rate = float(self.errors_apps) / self.processed_apps
        except ZeroDivisionError:
            err_rate = float('inf')
        return err_rate


class Worker(object):
    """Class used to process files in separate processes."""

    def __init__(self, dry, device_memc):
        self.dry = dry
        self.device_memc = device_memc
        self.max_threads = 4
        # this size must be close to useful packet size in network
        self.buff_max_size = 65000

    def __call__(self, fn):
        self.processing(fn, self.dry)

    def processing(self, filename, dry=False):
        """Process file."""
        processed_counter = ProcessedAppsCounter()
        pool = ThreadPool(self.max_threads)
        device_memc_clients = dict()
        memc_clients_buff = defaultdict(dict)
        memc_clients_buff_size = defaultdict(int)
        memc_clients_processed_apps = defaultdict(int)

        def bufferize_appsinstalled(memc_client, appsinstalled):
            """Buffize appsinstalled."""
            ua = UserApps()
            ua.lat = appsinstalled.lat
            ua.lon = appsinstalled.lon
            key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
            ua.apps.extend(appsinstalled.apps)
            packed = ua.SerializeToString()
            memc_clients_buff_size[memc_client] += len(packed)
            memc_clients_buff[memc_client].update({key: packed})
            memc_clients_processed_apps[memc_client] += 1
            return memc_clients_buff_size[memc_client]

        logging.info('Processing %s' % filename)

        for name, addr in self.device_memc.items():
            device_memc_clients[name] = Client([addr], socket_timeout=TIMEOUT)

        with gzip.open(filename) as fd:
            for line in fd:
                line = line.strip()
                if not line:
                    continue
                appsinstalled = parse_appsinstalled(line)
                if not appsinstalled:
                    processed_counter.error()
                    continue
                memc_client = device_memc_clients.get(appsinstalled.dev_type, None)

                if not memc_client:
                    processed_counter.error()
                    logging.error("Unknow device type: %s" % appsinstalled.dev_type)
                    continue

                # bufferize content and check buff size
                if bufferize_appsinstalled(memc_client, appsinstalled) > self.buff_max_size:
                    processed_counter.processed(count=memc_clients_processed_apps[memc_client])
                    # Get shallow copy
                    bufferized_dict = memc_clients_buff[memc_client].copy()
                    # Clean original buff
                    memc_clients_buff[memc_client] = dict()
                    # Reset size buffer
                    memc_clients_buff_size[memc_client] = 0
                    # Reset processed apps
                    memc_clients_processed_apps[memc_client] = 0

                    # Add task to pool
                    def to_excecute():
                        return insert_appsinstalled(memc_client, bufferized_dict, dry, processed_counter)
                    pool.map(to_excecute, [])

            # Send tailings from buffers
            for memc_client, bufferized_dict in memc_clients_buff.items():
                if memc_clients_buff_size[memc_client] > 0:
                    processed_counter.processed(count=memc_clients_processed_apps[memc_client])

                    # Add task to pool
                    def to_excecute():
                        return insert_appsinstalled(memc_client, bufferized_dict, dry, processed_counter)
                    pool.map(to_excecute, [])

            pool.close()
            pool.join()
            err_rate = processed_counter.get_errors_rate()
            processed = processed_counter.get_processed()
            if err_rate < NORMAL_ERR_RATE:
                logging.debug("processed %s" % processed)
                logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
            else:
                logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        dot_rename(filename)


def dot_rename(path):
    """Rename file."""
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def parse_appsinstalled(line):
    """Parse appsinstalled line."""
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def insert_appsinstalled(memc_client, bufferized_dict, error_counter, dry_run=False):
    """Insert appsinstalled."""
    try:
        if dry_run:
            logging.debug("%s -> %s" % (memc_client, bufferized_dict))
        else:
            memc_set(memc_client, bufferized_dict)

    except Exception as error:
        error_counter.error()
        logging.exception("Cannot write to memc %s: %s" % (memc_client, error))


def memc_set(client, key_value):
    """Set memc client."""
    res = None
    for _ in range(MAXATTEMPTS):
        try:
            res = client.set_multi(key_value)
        except Exception:
            pass
        if len(res) == 0:
            return True
        time.sleep(TIMEOUT)
    # if we are here then error has occured
    raise Exception('Max attempts exceeded while trying to connect to {} with error'.format(client))


def main(options):
    """Main function."""
    max_processes = int(options.processes)
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    process_pool = multiprocessing.Pool(max_processes)
    worker = Worker(options.dry, device_memc)
    process_pool.map(worker, [filename for filename in glob.iglob(options.pattern)])


def prototest():
    """Prototest."""
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    """Loader."""
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="memcache_load/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    op.add_option("--processes", action="store", default=4)
    (opts, args) = op.parse_args()
    logging.basicConfig(
        filename=opts.log,
        level=logging.INFO if not opts.dry else logging.DEBUG,
        format='[%(asctime)s] %(levelname).1s %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S',
    )
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
