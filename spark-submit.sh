#!/bin/bash

# Running Spark application on Kubernetes cluster
${SPARK_HOME}/bin/spark-submit \
  --name spark-basics \
  --class driver.Driver \
  --master k8s://https://bdcctislenkoaa-731b1076.hcp.westeurope.azmk8s.io:443  \
  --deploy-mode cluster \
  --conf spark.executor.instances=5 \
  --conf spark.kubernetes.container.image.pullPolicy=Always \
  --conf spark.kubernetes.container.image=bigbackclock/sparkbasics:1.0.0 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.driver.pod.name=spark-basics-driver \
  local:///opt/sparkbasics-1.0.0.jar