#!/usr/bin/env bash

rm -rf venv/

rm -rf /etc/auditor-apel-plugin/

unlink /usr/bin/auditor-apel-publish
unlink /usr/bin/auditor-apel-venv
unlink /etc/systemd/system/auditor-apel-plugin.service
