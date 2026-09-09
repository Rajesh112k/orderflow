#!/bin/bash

set -e

echo "=============================="
echo "Building OrderFlow"
echo "=============================="

echo "Step 1: Running Maven tests..."
./mvnw clean test

echo "Step 2: Packaging application..."
./mvnw package -DskipTests

echo "Step 3: Building Docker image..."
docker build -t orderflow:1.2 .

echo "Build completed successfully."