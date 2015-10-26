# ETL

# Deploy

Install MySQL server

Create table using table .sql

Setup MySQL setting in bonecp.config

mvn clean
mvn package

java -jar target etl-1.0-jar-with-dependencies.jar

# Amazon

Point elastic ip 52.23.42.33 to MySQL server

Make sure MySQL server has ran:

GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'burst_cmu';
FLUSH PRIVILEGES;

And in /etc/mysql/my.cfg this commented:
\#bind-address           = 127.0.0.1




