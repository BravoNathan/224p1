#!/bin/bash

for port in {8080..8095}; do
    lsof -i :$port | awk 'NR!=1 {print $2}' | xargs kill -9
done