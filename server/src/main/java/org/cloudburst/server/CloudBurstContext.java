package org.cloudburst.server;

import org.cloudburst.server.servlets.Q1Servlet;
import org.cloudburst.server.util.LoggingConfigurator;
import org.cloudburst.server.util.MySQLConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;
import java.util.Properties;
import java.util.TimeZone;

public class CloudBurstContext implements ServletContextListener {

    private final static Logger logger = LoggerFactory.getLogger(CloudBurstContext.class);

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        Properties boneCPConfigProperties = new Properties();
        Properties configProperties = new Properties();

        try {
            boneCPConfigProperties.load(CloudBurstContext.class.getResourceAsStream("/bonecp.properties"));
            configProperties.load(CloudBurstContext.class.getResourceAsStream("/config.properties"));
        } catch (IOException ex) {
            logger.error("Problem reading properties", ex);
        }
        Q1Servlet.setFirstLine(configProperties.getProperty("team.id"), configProperties.getProperty("team.aws.id"));
        TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT+4"));
        logger.info("Server started");
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        System.gc();
        java.beans.Introspector.flushCaches();
        MySQLConnectionFactory.shutdown();
    }

}

