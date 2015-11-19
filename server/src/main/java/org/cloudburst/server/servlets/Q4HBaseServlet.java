package org.cloudburst.server.servlets;

import java.io.IOException;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.cloudburst.server.services.HBaseService;
import org.cloudburst.server.util.HBaseConnectionFactory;

public class Q4HBaseServlet extends HttpServlet {

    private static final long serialVersionUID = 591010235959419459L;

    private HBaseService hbaseService = new HBaseService(new HBaseConnectionFactory());

    private static String FIRST_LINE;

    public static void setFirstLine(String teamId, String teamAWSId) {
        FIRST_LINE = teamId + "," + teamAWSId + "\n";
    }

    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);
        initHBaseService();
    }

    /**
     * Initialize HBase connection pool.
     */
    private void initHBaseService() {
        Properties hbaseConfigProperties = new Properties();
        try {
            hbaseConfigProperties.load(Q4HBaseServlet.class.getResourceAsStream("/hbase.properties"));
        } catch (IOException ex) {
            System.out.println(ex);
        }

        HBaseConnectionFactory.init(hbaseConfigProperties);
    }

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException {

        /* Parsing the request */
        String hashtag = request.getParameter("hashtag");
        long n = Long.parseLong(request.getParameter("n"));

        /* Generates the response. */
        StringBuilder finalMessage = new StringBuilder(FIRST_LINE);
        finalMessage.append(hbaseService.getQ4Record(hashtag, "hashtags", "data", n));

        response.setHeader("Content-Type", "text/plain; charset=UTF-8");
        try {
            response.getOutputStream().write(finalMessage.toString().getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
