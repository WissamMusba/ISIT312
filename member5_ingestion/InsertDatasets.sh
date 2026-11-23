#move files into vm shared folder
cd [ur_shared_folder path]
cp wc-product-export-28-5-2026-1779968644501.csv /home/bigdata/Desktop/
cp report-custom-2026-05-28.csv /home/bigdata/Desktop/
cd /home/bigdata/
$HADOOP_HOME/bin/hadoop fs -copyFromLocal /home/bigdata/Desktop/wc-product-export-28-5-2026-1779968644501.csv /datalake/raw/woocommerce/catalogue/ingest_date=$(date +%Y-%m-%d)/
$HADOOP_HOME/bin/hadoop fs -copyFromLocal /home/bigdata/Desktop/report-custom-2026-05-28.csv /datalake/raw/woocommerce/sales/ingest_date=$(date +%Y-%m-%d)/
$HADOOP_HOME/bin/hadoop fs -ls -R /datalake/raw/