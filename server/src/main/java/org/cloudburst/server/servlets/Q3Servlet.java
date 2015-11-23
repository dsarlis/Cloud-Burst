package org.cloudburst.server.servlets;

import java.io.IOException;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.cloudburst.server.services.MySQLService;
import org.cloudburst.server.util.MySQLConnectionFactory;

/**
 * Class that handles response for Q2.
 */
public class Q3Servlet extends HttpServlet {

    private static final long serialVersionUID = 7661709122294459584L;

    private static String FIRST_LINE;

    private MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());

    public static void setFirstLine(String teamId, String teamAWSId) {
        FIRST_LINE = teamId + "," + teamAWSId + "\n";
    }

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

        /* Parsing the request parameters. */
        final String start_date = request.getParameter("start_date");
        final String end_date = request.getParameter("end_date");
        final String userId = request.getParameter("userid");
        final String limit = request.getParameter("n");

        /* Gets the response from the SQL Service. */
        String outMessage = FIRST_LINE + mySQLService.getTweetImpactScore(start_date, end_date, userId, limit);

        response.setHeader("Content-Type", "text/plain; charset=UTF-8");
        response.getOutputStream().write(outMessage.toString().getBytes());
    }

}
