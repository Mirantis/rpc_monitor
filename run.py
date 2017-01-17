import argparse

from oslo_config import cfg
from monitor import RPCStateMonitor


def parse_args():
    parser = argparse.ArgumentParser(description='RPC State Monitor')
    parser.add_argument('--config-file', dest='config_file', type=str, help='Path to configuration file')
    args = parser.parse_args()
    return args


def run():
    args = parse_args()
    cfg.CONF(["--config-file", args.config_file])
    RPCStateMonitor()


run()
