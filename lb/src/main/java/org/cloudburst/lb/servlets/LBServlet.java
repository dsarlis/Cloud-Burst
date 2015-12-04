package org.cloudburst.lb.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class LBServlet extends HttpServlet {

    private static final long serialVersionUID = 8714773241900130953L;

    private static final String[] SERVERS = { "http://ec2-54-85-147-171.compute-1.amazonaws.com" };

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        String path = request.getRequestURI() + "?" + request.getQueryString();
        int serverNumber = 0;

        if (path.startsWith("/q6")) {
            final String tid = request.getParameter("tid");

            serverNumber = tid.hashCode() % SERVERS.length;
        } else {
            serverNumber = path.hashCode() % SERVERS.length;
        }

        response.setStatus(302);
        response.setHeader("Location", SERVERS[serverNumber] + path);
    }

}
