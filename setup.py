import os
import version
from setuptools import setup, find_packages


setup(name="zgres",
      version=version.getVersion(),
      packages=find_packages(),
      install_requires=['kazoo'],
      description="Database Connection and failover manager for PostgreSQL",
      entry_points={
          'console_scripts': [
              'zgres-apply = zgres.apply:apply_cli',
              'zgres-sync = zgres.sync:sync_cli',
              'zgres-deadman = zgres.deadman:deadman_cli',
              ],
          'zgres.sync': [
              'zgres-apply = zgres.apply:Plugin',
              'zookeeper = zgres.zookeeper:ZooKeeperSource',
              ],
          'zgres.deadman': [
              'apt = zgres.apt:AptPostgresqlPlugin',
              'zookeeper = zgres.zookeeper:ZooKeeperDeadmanPlugin',
              ],
          },
      include_package_data = True,
      zip_safe = True,
      )
