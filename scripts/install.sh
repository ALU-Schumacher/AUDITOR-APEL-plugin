#!/usr/bin/env bash

python -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install -e .

mkdir /etc/auditor-apel-plugin

ln -s $PWD/apel_plugin.cfg /etc/auditor-apel-plugin/apel_plugin.cfg
ln -s $PWD/scripts/publish.py /usr/bin/auditor-apel-publish
ln -s $PWD/venv/bin/activate /usr/bin/auditor-apel-venv
ln -s $PWD/auditor-apel-plugin.service /etc/systemd/system/auditor-apel-plugin.service
