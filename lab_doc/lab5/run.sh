#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Create a Docker volume for data persistence
echo "Creating Docker volume: homework1-heart-disease"
docker volume create --name homework1-heart-disease

# Create a Docker network for container communication
#echo "Creating Docker network: etl-database"
#docker network create etl-database

# Build PostgreSQL Docker image
echo "Building PostgreSQL Docker image from dockerfile-postgresql"
docker build -f dockerfiles/dockerfile-postgresql -t postgresql-image .

# Build Jupyter Docker image
echo "Building Jupyter Docker image from dockerfile-jupyter"
docker build -f dockerfiles/dockerfile-jupyter -t jupyter-image .

# Run PostgreSQL container with volume and network setup
echo "Starting PostgreSQL container"
docker run -d --network etl-database \
	              --name postgres-container \
		                    -v homework1-heart-disease:/var/lib/postgresql/data \
				                  -p 5432:5432 \
						                postgresql-image

# Run Jupyter container with volume and network setup
echo "Starting Jupyter container"
docker run -it --network etl-database \
	              --name etl-container \
		                    -v ./src:/app/src \
				                  -p 8888:8888 \
						                jupyter-image


