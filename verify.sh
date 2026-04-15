#!/bin/bash

# Smart Agro - Verification Script
# Run this to check if the entire pipeline is working

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== Smart Agro Pipeline Verification ===${NC}\n"

# 1. Check Docker services
echo -e "${YELLOW}[1] Checking Docker services...${NC}"
RUNNING_SERVICES=$(docker compose ps --services --filter "status=running" | wc -l)
echo "✓ Services running: $RUNNING_SERVICES/8"

if docker compose ps | grep -q "spark-master.*Up"; then
    echo -e "${GREEN}✓ Spark Master: UP${NC}"
else
    echo -e "${RED}✗ Spark Master: DOWN${NC}"
fi

if docker compose ps | grep -q "cassandra.*Up"; then
    echo -e "${GREEN}✓ Cassandra: UP${NC}"
else
    echo -e "${RED}✗ Cassandra: DOWN${NC}"
fi

if docker compose ps | grep -q "kafka.*Up"; then
    echo -e "${GREEN}✓ Kafka: UP${NC}"
else
    echo -e "${RED}✗ Kafka: DOWN${NC}"
fi

if docker compose ps | grep -q "grafana.*Up"; then
    echo -e "${GREEN}✓ Grafana: UP${NC}"
else
    echo -e "${RED}✗ Grafana: DOWN${NC}"
fi

# 2. Check Cassandra data
echo -e "\n${YELLOW}[2] Checking Cassandra data...${NC}"
ROWS=$(docker compose exec -T cassandra cqlsh -e "USE smartagro; SELECT COUNT(*) as count FROM window_metrics;" 2>/dev/null | tail -2 | head -1 | awk '{print $1}' || echo "0")

if [ "$ROWS" -gt 0 ]; then
    echo -e "${GREEN}✓ Data in Cassandra: $ROWS rows${NC}"
else
    echo -e "${YELLOW}⚠ No data in Cassandra yet. Run batch_job.py to load sample data.${NC}"
fi

# 3. Check Kafka Topics
echo -e "\n${YELLOW}[3] Checking Kafka topics...${NC}"
TOPIC_LIST=$(docker compose exec -T kafka bash -c "kafka-topics --bootstrap-server localhost:9092 --list" 2>/dev/null || true)

for topic in greenhouse.sensors.simulated greenhouse.sensors.csv greenhouse.sensors.aemet; do
    if echo "$TOPIC_LIST" | grep -q "$topic"; then
        echo -e "${GREEN}✓ Topic '$topic' exists${NC}"
    else
        echo -e "${YELLOW}⚠ Topic '$topic' not found. Run: python3 greenhouse/producer/setup_topic.py${NC}"
    fi
done

# 4. Check services accessibility
echo -e "\n${YELLOW}[4] Checking service endpoints...${NC}"

# Grafana
if curl -s -o /dev/null -w "%{http_code}" http://localhost:3000 | grep -q "200"; then
    echo -e "${GREEN}✓ Grafana: http://localhost:3000${NC}"
else
    echo -e "${YELLOW}⚠ Grafana: Not responding (expected if first run)${NC}"
fi

# Redpanda
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8090 | grep -q "200"; then
    echo -e "${GREEN}✓ Redpanda Console: http://localhost:8090${NC}"
else
    echo -e "${YELLOW}⚠ Redpanda: Not responding${NC}"
fi

# Spark UI
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080 | grep -q "200"; then
    echo -e "${GREEN}✓ Spark Master UI: http://localhost:8080${NC}"
else
    echo -e "${YELLOW}⚠ Spark UI: Not responding${NC}"
fi

# MinIO
if curl -s -o /dev/null -w "%{http_code}" http://localhost:9001 | grep -q "200"; then
    echo -e "${GREEN}✓ MinIO Console: http://localhost:9001${NC}"
else
    echo -e "${YELLOW}⚠ MinIO: Not responding${NC}"
fi

# 5. Recommendations
echo -e "\n${YELLOW}[5] Next Steps:${NC}"
echo "1. If Cassandra is empty:"
echo "   docker compose exec spark-master /opt/spark/bin/spark-submit /opt/project/greenhouse/spark/batch_job.py"
echo ""
echo "2. To emit real-time sensor data:"
echo "   python3 greenhouse/producer/producer_simulated.py --events-per-second 2 --max-events 500"
echo "   python3 greenhouse/producer/producer_csv.py --csv-path data/greenhouse_crop_yields.csv --events-per-second 2 --max-events 200"
echo "   python3 greenhouse/producer/producer_aemet.py --events-per-second 0.0033 --max-events 3"
echo ""
echo "3. Access interfaces:"
echo "   - Grafana:        http://localhost:3000 (admin/admin)"
echo "   - Redpanda:       http://localhost:8090"
echo "   - Spark UI:       http://localhost:8080"
echo "   - MinIO:          http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "4. Query Cassandra:"
echo "   docker compose exec -T cassandra cqlsh -e \"USE smartagro; SELECT * FROM window_metrics LIMIT 5;\""
echo ""

echo -e "${GREEN}=== Verification Complete ===${NC}\n"
