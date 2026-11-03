#!/usr/bin/env bash
set -e
ORDERS=${1:-10}
WORKERS=${2:-1}

docker compose up -d --scale worker="$WORKERS" rabbitmq worker
sleep 5

START=$(date +%s)
docker compose run --rm producer --count "$ORDERS" > /dev/null

while true; do
  LEFT=$(docker compose exec -T rabbitmq rabbitmqctl list_queues -q name messages | awk '$1=="orders"{print $2}')
  [ "${LEFT:-0}" = "0" ] && break
  sleep 1
done
END=$(date +%s)

echo "Замовлень: $ORDERS | Worker: $WORKERS | Час: $((END-START)) c | Залишилось у черзі: ${LEFT:-0}"