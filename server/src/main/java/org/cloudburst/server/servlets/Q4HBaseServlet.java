package org.cloudburst.server.servlets;

import org.cloudburst.server.services.HBaseService;
import org.cloudburst.server.util.HBaseConnectionFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Q4HBaseServlet extends HttpServlet {
    private static final int THREAD_POOL_SIZE = 100;
    private HBaseService hbaseService = new HBaseService(new HBaseConnectionFactory());
    private static ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

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
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException {

//        executorService.submit(new Runnable() {
//            @Override
//            public void run() {
                String hashtag = request.getParameter("hashtag");
                long n = Long.parseLong(request.getParameter("n"));

                StringBuilder finalMessage = new StringBuilder(FIRST_LINE);

                finalMessage.append(hbaseService.getQ4Record(hashtag, "hashtags", "data", n));

                response.setHeader("Content-Type", "text/plain; charset=UTF-8");
                try {
                    response.getOutputStream().write(finalMessage.toString().getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
//            }
//        });
    }
}
