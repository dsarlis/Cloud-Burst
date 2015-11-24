sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo aptitude -y install unzip oracle-java8-installer mysql-server htop
wget http://download.eclipse.org/jetty/stable-9/dist/jetty-distribution-9.3.6.v20151106.zip
unzip jetty-distribution-9.3.6.v20151106.zip
mv jetty-distribution-9.3.6.v20151106 jetty
sudo sed -i 's/default="8080"/default="80"/g' jetty/etc/jetty-http.xml
sudo echo "JETTY_HOME=/home/ubuntu/jetty" | sudo tee /etc/default/jetty
sudo cp jetty/bin/jetty.sh /etc/init.d/jetty
sudo update-rc.d jetty defaults

sudo mkdir /Q2
sudo mkdir /Q3
sudo mkdir /Q4

sudo mount /dev/xvdf /Q2
sudo mount /dev/xvdg /Q3
sudo mount /dev/xvdh /Q4

mysql -u root -pburst_cmu

create database 15619tp;

sudo cp /Q2/* /var/lib/mysql/15619tp/
sudo cp /Q3/* /var/lib/mysql/15619tp/
sudo cp /Q4/* /var/lib/mysql/15619tp/

sudo chmod -R 777 /var/lib/mysql/15619tp/

sudo umount /Q2
sudo umount /Q3
sudo umount /Q4


###########

aws ec2 attach-volume --volume-id vol-a809f44b --instance-id i- --device /dev/sdf
aws ec2 attach-volume --volume-id vol-4424dba7 --instance-id i- --device /dev/sdg
aws ec2 attach-volume --volume-id vol-721fe091 --instance-id i- --device /dev/sdh

aws ec2 detach-volume --volume-id vol-a809f44b
aws ec2 detach-volume --volume-id vol-4424dba7
aws ec2 detach-volume --volume-id vol-721fe091
