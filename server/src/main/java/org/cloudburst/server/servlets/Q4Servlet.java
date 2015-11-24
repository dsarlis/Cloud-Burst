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
 * Servlet for Q4.
 */
public class Q4Servlet extends HttpServlet {

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
        String hashTag = request.getParameter("hashtag");
        int limit = Integer.parseInt(request.getParameter("n"));

        String key = hashTag + limit;
        String result = cache.get(key);

        if (result == null) {
        /* Generating the response. */
            result = FIRST_LINE + mySQLService.getTopHashtags(hashTag, limit);
            if (cache.size() < 300000) {
                cache.put(key, result);
            }
        }

        response.setHeader("Content-Type", "text/plain; charset=UTF-8");
        response.getOutputStream().write(result.getBytes());
    }

}
