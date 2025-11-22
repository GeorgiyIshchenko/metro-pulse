#!/bin/bash

set -e

docker exec -it metropulse-spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/staging_load.py --batch-id test-000

docker exec -it metropulse-spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/dwh_load_dim_core.py

docker exec -it metropulse-spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/dwh_load_dim_route_scd2.py

docker exec -it metropulse-spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/dwh_load_facts.py

