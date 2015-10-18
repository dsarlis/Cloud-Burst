package org.cloudburst.server.servlets;

import org.cloudburst.server.services.MySQLService;
import org.cloudburst.server.util.MySQLConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class Q2Servlet extends HttpServlet {

    private final static Logger logger = LoggerFactory.getLogger(Q2Servlet.class);
    private MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());

    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);
    }

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        logger.info("CALLED!");
        response.getOutputStream().write(mySQLService.testStatement().getBytes());
    }

}
