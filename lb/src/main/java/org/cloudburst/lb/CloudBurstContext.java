package org.cloudburst.lb;

import java.lang.reflect.Field;
import java.nio.charset.Charset;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.cloudburst.lb.util.LoggingConfigurator;
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
        LoggingConfigurator.configureFor(LoggingConfigurator.Environment.PRODUCTION);
        forceUTF8Encoding();
        logger.info("Server started");
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
    }

}
