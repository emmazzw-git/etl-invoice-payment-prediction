#!/bin/bash

. "/opt/spark/bin/load-spark-env.sh"

if [ "$SPARK_WORKLOAD" == "master" ];
then

export SPARK_MASTER_HOST=`hostname`

cd /opt/spark/bin \
    && ./spark-class org.apache.spark.deploy.master.Master \
        --ip $SPARK_MASTER_HOST \
        --port $SPARK_MASTER_PORT \
        --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG

elif [ "$SPARK_WORKLOAD" == "worker" ];
then

cd /opt/spark/bin \
    && ./spark-class org.apache.spark.deploy.worker.Worker \
        --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG

elif [ "$SPARK_WORKLOAD" == "submit" ];
then

cd /opt/spark/bin && ./spark-submit --master spark://spark-master:7077 \
    --driver-memory 1G \
    --executor-memory 1G \
    /opt/spark-app/main.py \
    "local" \
    "etl-invoice-payment-prediction" \
    "['/opt/spark-data/accounts.csv', '/opt/spark-data/invoice_line_items.csv', '/opt/spark-data/invoices.csv', '/opt/spark-data/skus.csv']"

else
    echo "Undefined Workload Type $SPARK_WORKLOAD, must specify: master, worker, submit"
fi