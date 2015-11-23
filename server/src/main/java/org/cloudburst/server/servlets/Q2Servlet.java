package org.cloudburst.server.servlets;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
public class Q2Servlet extends HttpServlet {

    private static final long serialVersionUID = -6772179220153648509L;

    private static String FIRST_LINE;
    private MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());

    /* Using cache in front-end to reduce database overhead */
    private static Map<String, String> cache = new HashMap<String, String>();

    public static void setFirstLine(String teamId, String teamAWSId) {
        FIRST_LINE = teamId + "," + teamAWSId + "\n";
    }

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

        /* Parsing the request. */
        long userId = Long.valueOf(request.getParameter("userid"));
        String creationTime = request.getParameter("tweet_time");

        /* Preparing response, by first checking inside the cache. */
        String key = userId + creationTime;
        String result = cache.get(key);

        if (result == null) {
            StringBuilder finalMessage = new StringBuilder(FIRST_LINE);

            /* Gets the result for the query from the MySQLService. */
            finalMessage.append(mySQLService.getTweetResult(userId, creationTime));

            result = finalMessage.toString();
            if (cache.size() < 300000)
                cache.put(key, result);
        }

        response.setHeader("Content-Type", "text/plain; charset=UTF-8");
        response.getOutputStream().write(result.getBytes());
    }

}
