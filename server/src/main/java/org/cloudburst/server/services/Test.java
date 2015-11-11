package org.cloudburst.server.services;

import java.io.IOException;
import java.util.Properties;

import org.cloudburst.server.servlets.Q4Servlet;
import org.cloudburst.server.util.MySQLConnectionFactory;

public class Test {

    private static MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());

    public static void main(String[] args) {

        Properties boneCPConfigProperties = new Properties();
        try {
            boneCPConfigProperties.load(Q4Servlet.class.getResourceAsStream("/bonecp.properties"));
        } catch (IOException ex) {
        }
        MySQLConnectionFactory.init(boneCPConfigProperties);
        System.out.println(mySQLService.getTweetImpactScore("2014-03-26", "2014-05-18", "1000005894", "4"));
    }

}
