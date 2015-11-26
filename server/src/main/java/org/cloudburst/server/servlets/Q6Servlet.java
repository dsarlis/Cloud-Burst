package org.cloudburst.server.servlets;

import org.cloudburst.server.services.MySQLService;
import org.cloudburst.server.util.MySQLConnectionFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Servlet for Q6.
 */
public class Q6Servlet extends HttpServlet {

    private static final long serialVersionUID = -3974307160198438787L;

    private static final String[] SERVERS = {"172.31.13.42"};

    private static ConcurrentHashMap<Long, AtomicInteger> locks = new ConcurrentHashMap<Long, AtomicInteger>();

    private MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());

    private static Map<String, String> cache = new HashMap<String, String>();

    private static String FIRST_LINE;

    private static int myNumber;

    public static void setMyNumber() {
        try {
            String ip = Inet4Address.getLocalHost().getHostAddress();
            for (int i = 0; i < SERVERS.length; i++) {
                if (ip.equals(SERVERS[i])) {
                    myNumber = i;
                    break;
                }
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static void setFirstLine(String teamId, String teamAWSId) {
        FIRST_LINE = teamId + "," + teamAWSId + "\n";
    }


    private void waitForCorrectSeq(long tid, int seq) {
        while (true) {
            if (locks.containsKey(tid)) {
                if (locks.get(tid).get() == seq) {
                    if (seq == 5) {
                        locks.remove(tid);
                    } else {
                        AtomicInteger lock = locks.get(tid);
                        lock.getAndIncrement();
                        locks.put(tid, lock);
                    }
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
        int serverNumber = 0;

        if (opt.equals("s") || opt.equals("e")) {
            result += "0\n";
            response.getOutputStream().write(result.getBytes());
        } else {
            long tweetId = Long.parseLong(request.getParameter("tweetId"));
            serverNumber = (tweetId + "").hashCode() % SERVERS.length;
            String path = request.getRequestURI() + "?" + request.getQueryString();

            /* send it to the correct server */
            if (serverNumber != myNumber) {
                response.setStatus(302);
                response.setHeader("Location", "http://" + SERVERS[serverNumber] + path);
                return;
            }

            /* the value is for the current server */
            int seq = Integer.parseInt(request.getParameter("seq"));
            if (seq == 1) {
                locks.put(tid, new AtomicInteger(1));
            }

            waitForCorrectSeq(tid, seq);

            if (opt.equals("a")) {
                /* handle append operation */
                String tag = request.getParameter("tag");

                if (mySQLService.appendTag(tweetId, tag)) {
                    result += tag + "\n";
                    response.getOutputStream().write(result.getBytes());
                } else {
                    System.err.println("Problem while appending");
                }
            } else {
                /* handle read operation */
                result += mySQLService.readTweetWithTag(tweetId);
                response.getOutputStream().write(result.getBytes());
            }
        }
    }

}
