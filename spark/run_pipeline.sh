#!/bin/bash

set -e

docker exec metropulse-spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/staging_load.py --batch-id test-000

docker exec metropulse-spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/dwh_load_dim_core.py

docker exec metropulse-spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/dwh_load_dim_route_scd2.py

docker exec metropulse-spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/dwh_load_facts.py

docker exec metropulse-spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/build_trip_mart_clickhouse.py --reload-all
