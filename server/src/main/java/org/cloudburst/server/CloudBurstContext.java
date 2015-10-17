package org.cloudburst.server;

import org.cloudburst.server.util.LoggingConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class CloudBurstContext implements ServletContextListener {

    private final static Logger logger = LoggerFactory.getLogger(CloudBurstContext.class);

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        LoggingConfigurator.configureFor(LoggingConfigurator.Environment.DEVELOPMENT);
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        System.gc();
        java.beans.Introspector.flushCaches();
    }

}
