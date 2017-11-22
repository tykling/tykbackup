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
                    self.logger.debug("processing target %s" % target)
                    # check if target fs exists
                    remote = self.get_or_create_dataset("zfs://%(hostname)s/%(targetfs)s/%(dataset)s" % (target.hostname, target.targetfs))

                    if self.get_daily_snapshot(remote):
                        self.logger.debug("the target %s already has the snapshot %s" % (target, snapshot.name))
                        continue

                    # this target does not have todays snapshot for this dataset,
                    # check if target has any previous daily snapshots so we can send incremental
                    remote_daily_snaps = sorted(snap.name for snap in zfs.open("zfs://bong.tyknet.dk/tank/root").snapshots() if "daily-" in snap.name)

                    if remote_daily_snaps:
                        # we have a daily snapshot on the remote node,
                        # do an incremental send, include all intermediate
                        # snapshots (weekly, monthly, yearly),
                        # send based on the latest remote daily snapshot
                        latest_remote_snap = zfs.open(remote_daily_snaps[-1])
                        local_counterpart = self.remote_to_local_name(latest_remote_snap)
                        sender = snapshot.send(base=local_counterpart, intermediates=True)
                    else:
                        # this target has no daily snapshots for this dataset,
                        # we cannot do an incremental send,
                        # do a full send
                        sender = snapshot.send()
                    #reciever = zfs.receive(remotestring)
                    #while True:
                    #    receiver.write(sender.read(1024))
                    #    print("wrote 1024 bytes to receiver")
                    print("would have sent snapshot %s to receiver %s here..." % (snapshot.name, remotestring))


    def remote_to_local_snap(self, remotestring, target):
        """
        Return the local counterpart of a remote snapshot
        """
        pass

    def get_or_create_dataset(self, name):
        # open dataset
        try:
            dataset = zfs.open(dataset)
        except process.DatasetNotFoundError:
            self.logger.exception("Unable to open dataset: %s - creating..." % remotestring)
            dataset = zfs.create(name, force=True)
        return dataset

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

