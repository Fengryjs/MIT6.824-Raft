#!/bin/bash

if [ $# -lt 1 ]; then
  echo "Usage: $0 <run_times> [command_args]"
  exit 1
fi

run_times=$1
shift

if [ $# -gt 0 ]; then
  command="time go test -run $@"
else
  command="time go test"
fi

for ((i=0; i<$run_times; i++))
do
  echo "Running test $((i+1))..."
  $command
  if [ $? -ne 0 ]; then
    echo "Test failed, exiting loop."
    exit 1
  fi
done
echo "... Pass all $((i+1)) test ..."