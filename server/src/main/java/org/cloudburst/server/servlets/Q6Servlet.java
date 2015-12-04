package org.cloudburst.server.servlets;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.cloudburst.server.services.MySQLService;
import org.cloudburst.server.util.MySQLConnectionFactory;

/**
 * Servlet for Q6.
 */
public class Q6Servlet extends HttpServlet {

    private static final long serialVersionUID = -3974307160198438787L;

    /* List of server internal IPs */
    private static final String[] SERVERS = { "172.31.6.27", "172.31.10.59", "172.31.4.41", "172.31.12.135",
            "172.31.4.28", "172.31.5.41", "172.31.10.244", "172.31.6.22" };

    /* List of server DNS names */
    private static final String[] DNS_NAMES = { "ec2-54-173-118-126.compute-1.amazonaws.com",
            "ec2-54-173-233-159.compute-1.amazonaws.com", "ec2-54-85-128-4.compute-1.amazonaws.com",
            "ec2-52-91-143-172.compute-1.amazonaws.com", "ec2-54-85-144-15.compute-1.amazonaws.com",
            "ec2-107-21-45-199.compute-1.amazonaws.com", "ec2-54-173-10-52.compute-1.amazonaws.com",
            "ec2-54-88-16-7.compute-1.amazonaws.com" };

    /* data structure to hold the locks for each transaction id */
    private static ConcurrentHashMap<Long, AtomicInteger> locks = new ConcurrentHashMap<Long, AtomicInteger>();

    private MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());

    private static Map<String, String> cache = new HashMap<String, String>();

    private static String FIRST_LINE;

    private static int myNumber;

    /*
     * method to find out the number of your server instance so the server knows
     * what keys it needs to process and what keys it needs to redirect to other
     * server instances
     */
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

    /*
     * Function to wait until it is your turn to handle the request Essentially,
     * we use an AtomicInteger lock to order the requests for the same
     * transcation id by their sequence id So seq=2 will wait for seq=1, seq=3
     * will wait for seq=1 and seq=2 to be completed and so on
     */
    private void waitForCorrectSeq(long tid, int seq) {
        while (true) {
            if (locks.containsKey(tid)) {
                if (locks.get(tid).get() == seq) {
                    if (seq == 5) {
                        locks.remove(tid);
                    } else {
                        locks.get(tid).getAndIncrement();
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
        String opt = request.getParameter("opt");

        response.setHeader("Content-Type", "text/plain; charset=UTF-8");
        String result = FIRST_LINE;
        int serverNumber = 0;

        if (opt.equals("s") || opt.equals("e")) {
            result += "0\n";
            response.getOutputStream().write(result.getBytes());
        } else {
            /*
             * find out the server that is responsible for this transaction id
             */
            serverNumber = (int) (tid % SERVERS.length);
            String path = request.getRequestURI() + "?" + request.getQueryString();

            /* send it to the correct server */
            if (serverNumber != myNumber) {
                response.setStatus(302);
                response.setHeader("Location", "http://" + DNS_NAMES[serverNumber] + path);
                return;
            }

            /* the value is for the current server */
            long tweetId = Long.parseLong(request.getParameter("tweetid"));
            int seq = Integer.parseInt(request.getParameter("seq"));
            if (seq == 1) {
                locks.put(tid, new AtomicInteger(1));
            }

            /* wait for your turn to be executed */
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
