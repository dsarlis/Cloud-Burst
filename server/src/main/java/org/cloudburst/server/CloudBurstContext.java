package org.cloudburst.server;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.TimeZone;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.cloudburst.server.servlets.*;
import org.cloudburst.server.util.MySQLConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet context where everything is initialized.
 */
public class CloudBurstContext implements ServletContextListener {

    private final static Logger logger = LoggerFactory.getLogger(CloudBurstContext.class);

    /**
     * Gets called when the server starts.
     */
    public void contextInitialized(ServletContextEvent servletContextEvent) {

        /*
         * Loads all property files, and puts in the context. Makes it easier
         * for global access.
         */
        Properties boneCPConfigProperties = new Properties();
        Properties configProperties = new Properties();

        try {
            boneCPConfigProperties.load(CloudBurstContext.class.getResourceAsStream("/bonecp.properties"));
            configProperties.load(CloudBurstContext.class.getResourceAsStream("/config.properties"));
        } catch (IOException ex) {
            logger.error("Problem reading properties", ex);
        }

        forceUTF8Encoding();
        MySQLConnectionFactory.init(boneCPConfigProperties);
        addTeamHeaderToServletResponse(configProperties);

        TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT+4"));
        logger.info("Server started");
    }

    /**
     * Pulled up the teamId and the AWS.Id in this method. All servlets are
     * given the team headers in the context itself.
     */
    private void addTeamHeaderToServletResponse(Properties configProperties) {
        final String teamId = configProperties.getProperty("team.id");
        final String awsId = configProperties.getProperty("team.aws.id");

        Q1Servlet.setFirstLine(teamId, awsId);
        Q2Servlet.setFirstLine(teamId, awsId);
        Q3Servlet.setFirstLine(teamId, awsId);
        Q4Servlet.setFirstLine(teamId, awsId);
        Q5Servlet.setFirstLine(teamId, awsId);
    }

    /**
     * Forces UTF8 charset to the linux machines.
     */
    private void forceUTF8Encoding() {
        System.setProperty("file.encoding", "UTF-8");
        Field charset = null;
        try {
            charset = Charset.class.getDeclaredField("defaultCharset");
            charset.setAccessible(true);
            charset.set(null, null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets called when the server is gracefully shutdown.
     */
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        System.gc();
        java.beans.Introspector.flushCaches();

        /* Shutting down the connection pool for MySQL. */
        MySQLConnectionFactory.shutdown();
    }

}
