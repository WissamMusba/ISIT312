$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode
$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
$HADOOP_HOME/sbin/yarn-daemon.sh start resourcemanager
$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver
$HADOOP_HOME/bin/hadoop fs -mkdir -p /datalake/raw/woocommerce/catalogue/ingest_date=$(date +%Y-%m-%d)
$HADOOP_HOME/bin/hadoop fs -mkdir -p /datalake/raw/woocommerce/sales/ingest_date=$(date +%Y-%m-%d)
$HADOOP_HOME/bin/hadoop fs -mkdir -p /datalake/cleansed/catalogue
$HADOOP_HOME/bin/hadoop fs -mkdir -p /datalake/cleansed/sales
$HADOOP_HOME/bin/hadoop fs -mkdir -p /datalake/cleansed/_quarantine
$HADOOP_HOME/bin/hadoop fs -mkdir -p /datalake/curated/runs/run_001/dim_product
$HADOOP_HOME/bin/hadoop fs -mkdir -p /datalake/curated/runs/run_001/dim_brand
$HADOOP_HOME/bin/hadoop fs -mkdir -p /datalake/curated/runs/run_001/dim_category
$HADOOP_HOME/bin/hadoop fs -mkdir -p /datalake/curated/runs/run_001/dim_date
$HADOOP_HOME/bin/hadoop fs -mkdir -p /datalake/curated/runs/run_001/fact_sales_monthly
$HADOOP_HOME/bin/hadoop fs -mkdir -p /datalake/_meta
