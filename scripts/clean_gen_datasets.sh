#!/bin/bash

HOSTS=("192.168.100.2" "192.168.100.3" "192.168.100.4" "192.168.100.5")

for host in "${HOSTS[@]}"; do
    echo "Cleaning gen_datasets on root@$host ..."
    ssh root@"$host" "rm -rf /root/cxl_bandle/gen_datasets/*"
    echo "Done: $host"
done

echo "All hosts cleaned."
