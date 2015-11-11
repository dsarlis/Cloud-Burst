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
 * Servlet for Q3.
 */
public class Q3Servlet extends HttpServlet {

    private MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());

    private static String FIRST_LINE;

    public static void setFirstLine(String teamId, String teamAWSId) {
        FIRST_LINE = teamId + "," + teamAWSId + "\n";
    }

    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);
        initMySqlService();
    }

    /**
     * Initialize MySQL connection pool.
     */
    private void initMySqlService() {
        Properties boneCPConfigProperties = new Properties();
        try {
            boneCPConfigProperties.load(Q3Servlet.class.getResourceAsStream("/bonecp.properties"));
        } catch (IOException ex) {
        }

        MySQLConnectionFactory.init(boneCPConfigProperties);
    }

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

        final String start_date = request.getParameter("start_date");
        final String end_date = request.getParameter("end_date");
        final String userId = request.getParameter("userid");
        final String limit = request.getParameter("n");

        String outMessage = FIRST_LINE + mySQLService.getTweetImpactScore(start_date, end_date, userId, limit);

        response.setHeader("Content-Type", "text/plain; charset=UTF-8");
        response.getOutputStream().write(outMessage.toString().getBytes());
    }

}
