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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Servlet for Q6.
 */
public class Q6Servlet extends HttpServlet {

    private static final long serialVersionUID = -3974307160198438787L;

    private static ConcurrentHashMap<Long, AtomicInteger> locks = new ConcurrentHashMap<Long, AtomicInteger>();

    private MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());

    private static Map<String, String> cache = new HashMap<String, String>();

    private static String FIRST_LINE;

    public static void setFirstLine(String teamId, String teamAWSId) {
        FIRST_LINE = teamId + "," + teamAWSId + "\n";
    }

    private void waitForCorrectSeq(long tid, int seq) {
        while (true) {
            if (locks.containsKey(tid)) {
                if (locks.get(tid).get() == seq) {
                    AtomicInteger lock = locks.get(tid);
                    lock.getAndIncrement();
                    locks.put(tid, lock);
                    return;
                }
            }
        }
    }

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

        /* Parsing the request. */
        long tid = Long.parseLong(request.getParameter("tid"));
        String opt = request.getParameter("s");

        response.setHeader("Content-Type", "text/plain; charset=UTF-8");
        String result = FIRST_LINE;

        if (opt.equals("s") || opt.equals("e")) {
            result += "0\n";
            response.getOutputStream().write(result.getBytes());
        } else {
            long tweetId = Long.parseLong(request.getParameter("tweetId"));

            // TODO
            /* compute hash value based on tweetId and send to the correct server */

            /* the value is for the current server */
            int seq = Integer.parseInt(request.getParameter("seq"));
            if (seq == 1) {
                locks.put(tid, new AtomicInteger(1));
            }

            waitForCorrectSeq(tid, seq);

            if (opt.equals("a")) {
                /* handle append operation */
                String tag = request.getParameter("tag");

                mySQLService.appendTag(tweetId, tag);

                result += tag + "\n";
                response.getOutputStream().write(result.getBytes());
            } else {
                /* handle read operation */
                result += mySQLService.readTweetWithTag(tweetId);
                response.getOutputStream().write(result.getBytes());
            }
        }
    }

}
