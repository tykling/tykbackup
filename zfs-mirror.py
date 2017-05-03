#!/usr/local/bin/python
from pid import PidFile
import yaml, logging, sys, os, pwd, subprocess
from weir import zfs, process
from datetime import datetime
logging.basicConfig(level=logging.DEBUG)


class ZfsMirror:
    def __init__(self):
        """
        Parse yaml config file and do some housekeeping
        """
        self.logger = logging.getLogger()

        if not self.read_config():
            sys.exit(1)

        if not self.config['targets']:
            self.logger.error("No backup targets found in config - parsed config object:")
            print(self.config)
            sys.exit(1)

        self.dailystring = datetime.now().strftime("daily-%Y-%m-%d")


    def read_config(self):
        """
        Actually reads and parses the yaml config file
        """
        with open("zfsmirror.yml", "r") as f:
            try:
                self.config = yaml.load(f)
                return True
            except Exception as E:
                self.logger.exception("Unable to read config")
                return False

    def disco(self):
        for pool in self.config['pools']:
            self.logger.info("Processing pool %s" % pool)
            # open the root dataset on this pool
            try:
                rootds = zfs.open("zfs://bong.tyknet.dk/tyktank")
            except process.DatasetNotFoundError:
                self.logger.exception("Unable to open pool: %s" % pool)
                sys.exit(1)

            # build a list of all datasets in this pool
            datasets = [rootds]
            datasets += self.get_child_datasets(rootds)

            # loop through individual datasets in this pool
            for dataset in datasets:
                if dataset._url.path in self.config.skip_datasets:
                    self.logger.debug("skipping dataset %s because it is in skip_datasets" % dataset.name)
                    continue

                # find out if we have a local daily- snapshot for today for this dataset
                snapshot = self.get_daily_snapshot(dataset):
                if not snapshot:
                    self.logger.error("No local daily snapshot found for dataset %s so we have nothing to send!" % dataset.name)
                    continue

                # daily snapshot found, check remotes
                for target in targets:
                    if 'username' in target:
                        userstring = "%s@" % target.username
                    else:
                        userstring = ""
                    remotestring = "zfs://%(userstring)s%(hostname)s/%(targetfs)s/%(dataset)s" % (target.username, target.hostname, target.targetfs, dataset._url.path)
                    self.logger.debug("remotestring is %s" % remotestring)

                    # open remote dataset
                    try:
                        remote = zfs.open(remotestring)
                    except process.DatasetNotFoundError:
                        self.logger.exception("Unable to open remote pool: %s" % remotestring)
                        sys.exit(1)

                    if self.get_daily_snapshot(remote):
                        # this target already has this snapshot, continue with the next target
                        continue

                    # this target does not have todays snapshot for this dataset, send it!
                    sender = snapshot.send()
                    reciever = zfs.receive(remotestring)
                    while True:
                        receiver.write(sender.read(1024))
                        print("wrote 1024 bytes to receiver")
                    print("done")


    def get_daily_snapshot(self, dataset):
        for snapshot in dataset.snapshots():
            if snapshot.name == self.dailystring:
                return snapshot
        return False


    def get_child_datasets(self, parent, recursive=True):
        # build and return a list of children (and recursively their children)
        children = []
        for child in parent.filesystems():
            children += [child]
            if recursive:
                children += self.get_child_datasets(child)
        return children


if __name__ == '__main__':
    """
    Main method. 
    Maintain a pidfile to prevent multiple instances running.
    """
    with PidFile(piddir="/tmp/"):
        zfsmirror = ZfsMirror()
        zfsmirror.disco()

