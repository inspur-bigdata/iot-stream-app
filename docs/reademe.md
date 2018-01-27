# spark on k8s运行
bin/spark-submit \
  --deploy-mode cluster \
  --class myspark.Streaming22Sql \
  --master k8s://http://10.110.17.232:8080 \
  --kubernetes-namespace default \
  --conf spark.executor.instances=2 \
  --conf spark.app.name=spark-iot \
  --conf spark.kubernetes.driver.docker.image=registry.jiadx.com:5000/kubespark/spark-driver:v2.2.0-kubernetes-0.5.0 \
  --conf spark.kubernetes.executor.docker.image=registry.jiadx.com:5000/kubespark/spark-executor:v2.2.0-kubernetes-0.5.0 \
  --conf spark.kubernetes.initcontainer.docker.image=registry.jiadx.com:5000/kubespark/spark-init:v2.2.0-kubernetes-0.5.0 \
  --conf spark.kubernetes.resourceStagingServer.uri=http://10.110.17.232:31000 \
  /cephdata/iot-stream-app-1.0-SNAPSHOT-jar-with-dependencies.jar 

# Kill任务
查看状态
bin/spark-submit --master k8s://http://10.110.17.232:8080 --status spark-4b3d569ac4a44994bd7237aa4d6db13c
结束任务


# spark on yarn
  bin/spark-submit \
  --deploy-mode client \
  --class myspark.Streaming22Sql \
  --master yarn \
  --conf spark.executor.instances=2 \
  --conf spark.app.name=spark-iot \
  /cephdata/iot-stream-app-1.0-SNAPSHOT-jar-with-dependencies.jar 

# MQTT Spark SQL 
https://bahir.apache.org/docs/spark/2.0.0/spark-sql-streaming-mqtt/ 