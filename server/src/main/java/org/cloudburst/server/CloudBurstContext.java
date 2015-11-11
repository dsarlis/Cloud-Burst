package org.cloudburst.server;

import java.io.IOException;
import java.util.Properties;
import java.util.TimeZone;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.cloudburst.server.servlets.Q1Servlet;
import org.cloudburst.server.servlets.Q2Servlet;
import org.cloudburst.server.servlets.Q3Servlet;
import org.cloudburst.server.servlets.Q4Servlet;
import org.cloudburst.server.util.MySQLConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet context where everything is initialized.
 */
public class CloudBurstContext implements ServletContextListener {

    private final static Logger logger = LoggerFactory.getLogger(CloudBurstContext.class);

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        Properties boneCPConfigProperties = new Properties();
        Properties configProperties = new Properties();
        Properties hbaseConfigProperties = new Properties();

        try {
            boneCPConfigProperties.load(CloudBurstContext.class.getResourceAsStream("/bonecp.properties"));
            configProperties.load(CloudBurstContext.class.getResourceAsStream("/config.properties"));
            hbaseConfigProperties.load(CloudBurstContext.class.getResourceAsStream("/hbase.properties"));
        } catch (IOException ex) {
            logger.error("Problem reading properties", ex);
        }

        addTeamHeaderToServletResponse(configProperties);

        TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT+4"));
        logger.info("Server started");
    }

    private void addTeamHeaderToServletResponse(Properties configProperties) {
        final String teamId = configProperties.getProperty("team.id");
        final String awsId = configProperties.getProperty("team.aws.id");

        Q1Servlet.setFirstLine(teamId, awsId);
        Q2Servlet.setFirstLine(teamId, awsId);
        Q3Servlet.setFirstLine(teamId, awsId);
        Q4Servlet.setFirstLine(teamId, awsId);
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        System.gc();
        java.beans.Introspector.flushCaches();
        MySQLConnectionFactory.shutdown();
    }

}
