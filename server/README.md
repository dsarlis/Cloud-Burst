# Server

# Dependencies

* Maven
* Jetty
* MySQL
* HBase
* Java 8

sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer

sudo apt-get install mysql-server maven

wget http://download.eclipse.org/jetty/stable-9/dist/jetty-distribution-9.3.5.v20151012.zip
unzip jetty-distribution-9.3.5.v20151012.zip
mv jetty-distribution-9.3.5.v20151012 jetty

# MySQL

At the moment I have this setup:
username: root
password: password
port: 3306

# Logs

sudo mkdir -p /cloud-burst-server/logs
sudo chmod 777 -R /cloud-burst-server

If you want to check the logs:

tail -f /cloud-burst-server/logs/\*

# Generating war file

mvn clean
mvn package 

cp target/server-1.0.war route/to/jetty/webapps/ROOT.war

Go to Jetty bin folder and run
./jetty start
