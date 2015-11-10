sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo aptitude -y install unzip oracle-java8-installer
wget http://download.eclipse.org/jetty/stable-9/dist/jetty-distribution-9.3.5.v20151012.zip
unzip jetty-distribution-9.3.5.v20151012.zip
mv jetty-distribution-9.3.5.v20151012 jetty
sudo sed -i 's/default="8080"/default="80"/g' jetty/etc/jetty-http.xml
sudo echo "JETTY_HOME=/home/ubuntu/jetty" | sudo tee /etc/default/jetty
sudo cp jetty/bin/jetty.sh /etc/init.d/jetty
sudo update-rc.d jetty defaults
