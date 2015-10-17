package org.cloudburst.server.servlets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class BurstServlet extends HttpServlet {

    private final static Logger logger = LoggerFactory.getLogger(BurstServlet.class);


    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);
    }

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        String uri = request.getRequestURI();

        try {
            uri = URLDecoder.decode(uri, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            logger.warn("Unsupported encoding");
        }
        uri = uri.toLowerCase();

        if (uri.startsWith("/1")) {
            response.getOutputStream().write("1".getBytes());
        } else  {
            response.getOutputStream().write("2".getBytes());
        }
    }


}
