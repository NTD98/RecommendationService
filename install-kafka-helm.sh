#!/bin/bash
# Stop docker-compose if running to avoid port conflicts
if [ -f "docker-compose.yml" ]; then
    echo "Stopping docker-compose services..."
    docker-compose down
fi

# Uninstall existing helm release to avoid "in progress" errors or conflicts
echo "Cleaning up previous Kafka release..."
helm uninstall kafka --wait || true

# Install Kafka using Helm
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm upgrade --install kafka bitnami/kafka -f kafka-values.yaml --wait
