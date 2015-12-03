package org.cloudburst.server.servlets;

import org.cloudburst.server.services.MySQLService;
import org.cloudburst.server.util.MySQLConnectionFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Servlet for Q5.
 */
public class Q5Servlet extends HttpServlet {

    private static final long serialVersionUID = -3974307160198438787L;

    private MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());

    private static Map<String, String> cache = new HashMap<String, String>();

    private static String FIRST_LINE;

    public static void setFirstLine(String teamId, String teamAWSId) {
        FIRST_LINE = teamId + "," + teamAWSId + "\n";
    }

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

        /* Parsing the request. */
        long userIdMin = Long.parseLong(request.getParameter("userid_min"));
        long userIdMax = Long.parseLong(request.getParameter("userid_max"));

        String key = userIdMin+ "_" + userIdMax;
        /* Try to get the result from in-memory cache */
        String result = cache.get(userIdMin+ "_" + userIdMax);

        if (result == null) {
            /* Generating the response. */
            result = FIRST_LINE + mySQLService.getTotalTweets(userIdMin, userIdMax);
            /* Cache results in-memory to speed up the actual queries */
            if (cache.size() < 300000) {
                cache.put(key, result);
            }
        }

        /* return response */
        response.setHeader("Content-Type", "text/plain; charset=UTF-8");
        response.getOutputStream().write(result.getBytes());
    }

}
