#!/bin/bash
# Stop containers and remove volumes to fix INCONSISTENT_CLUSTER_ID
docker-compose down -v

# Start fresh
docker-compose up -d