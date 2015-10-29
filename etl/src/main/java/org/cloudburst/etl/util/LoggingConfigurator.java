package org.cloudburst.etl.util;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File to configure logging. It read logback configuration.
 */
public class LoggingConfigurator {

    private static final String PRODUCTION_PATH = "/production_logback.xml";
    private static final String DEVELOPMENT_PATH = "/development_logback.xml";

    public enum Environment {
        PRODUCTION(PRODUCTION_PATH),
        DEVELOPMENT(DEVELOPMENT_PATH);

        private String path;

        Environment(String path) {
            this.path = path;
        }
    }

    private static final Logger uncaughtExceptionLogger = LoggerFactory.getLogger("UNCAUGHT_EXCEPTION_HANDLER");

    private static boolean configured;

    public static void configureFor(Environment environment) {
        configureWithPath(environment.path);
    }

    private static void configureWithPath(String pathToConfig) {
        if (!configured) {
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

            try {
                JoranConfigurator configurator = new JoranConfigurator();
                configurator.setContext(context);
                context.reset();
                configurator.doConfigure(LoggingConfigurator.class.getResource(pathToConfig));

                Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        uncaughtExceptionLogger.error("An error occurred in thread {}", t.getName(), e);
                    }
                });

                configured = true;
            } catch (JoranException je) {
                System.err.println("Unable to configure Logback");
                je.printStackTrace();
            }
        } else {
            throw new IllegalStateException("Logging has already been configured!");
        }
    }

}
