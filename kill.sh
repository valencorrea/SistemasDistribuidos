#!/bin/bash

# Find and kill all processes with 'docker' in the command (excluding this script)
ps aux | grep docker | grep -v grep | grep -v "$0" | awk '{print $2}' | sudo xargs -r kill -9