#!/usr/bin/env bash

while getopts y:m:s: flag
do
    case "${flag}" in
	y) year=${OPTARG};;
	m) month=${OPTARG};;
	s) site=${OPTARG};;
    esac
done

source venv/bin/activate
python scripts/republish.py -y $year -m $month -s $site
