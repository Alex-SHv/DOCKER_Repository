set -e
cd "$(dirname "$0")"
COUNT=${1:-100}
WORKERS=${2:-1}

if [ "$COUNT" -le 0 ]; then
  echo "Кількість показників має бути більшою за 0"
  exit 1
fi

docker compose down -v >/dev/null 2>&1 || true
rm -f data/alerts.log

MESSAGE_COUNT="$COUNT" docker compose up -d --build --scale worker="$WORKERS"

docker wait "$(docker compose ps -q producer)" >/dev/null

queue_len() {
  docker compose exec -T rabbitmq rabbitmqctl list_queues -q name messages \
    | awk -v q="$1" '$1 == q {print $2}'
}

for _ in $(seq 1 600); do
  S=$(queue_len industrial_sensors || true)
  A=$(queue_len critical_alerts || true)
  if [ "${S:-0}" = "0" ] && [ "${A:-0}" = "0" ]; then
    break
  fi
  sleep 1
done

docker compose stop worker >/dev/null

SENT=$(docker compose logs --no-color producer | grep -c "Надіслано:" || true)
NORMAL=$(docker compose logs --no-color worker | grep -c "\[INFO\]" || true)
WARN=$(docker compose logs --no-color worker | grep -c "\[WARNING\]" || true)
CRIT=$(docker compose logs --no-color worker | grep -c "\[CRITICAL\]" || true)
URGENT=$(docker compose logs --no-color alert-handler | grep -c "\[URGENT\]" || true)

echo ""
echo "Показників надіслано:           $SENT"
echo "Норма        [INFO]:            $NORMAL"
echo "Попередження [WARNING]:         $WARN"
echo "Аварія       [CRITICAL]:        $CRIT"
echo "Сума категорій:                 $((NORMAL + WARN + CRIT))"
echo "Аварій отримав alert-handler:   $URGENT"
echo "Залишок у черзі: industrial_sensors=$(queue_len industrial_sensors || true), critical_alerts=$(queue_len critical_alerts || true)"
echo ""
echo "Підсумок по worker:"
docker compose logs --no-color worker | grep "Підсумок" || true
