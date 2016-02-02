#!/bin/bash

ulimit -c 1000000
sudo sysctl -w kernel.core_pattern=/var/core/%e.%p.%u.core
./segfault
