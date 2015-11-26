package org.cloudburst.lb.servlets;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class LBServlet extends HttpServlet {

    private static final String[] SERVERS = {"http://ec2-54-85-147-171.compute-1.amazonaws.com"};

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        String path = request.getRequestURI() + "?" + request.getQueryString();
        int serverNumber = 0;

        if (path.startsWith("/q6")) {
            final String tid = request.getParameter("tid");

            serverNumber = tid.hashCode() % SERVERS.length;
        } else {
            serverNumber = path.hashCode() % SERVERS.length;
        }

        response.setStatus(302);
        response.setHeader("Location", SERVERS[serverNumber] + path);
    }

}

//package org.cloudburst.lb.servlets;
//
//import org.apache.http.HttpResponse;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.impl.client.CloseableHttpClient;
//import org.cloudburst.lb.util.CloseableHttpClientFactory;
//
//import javax.servlet.ServletException;
//import javax.servlet.http.HttpServlet;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//
//public class LBServlet extends HttpServlet {
//
//    private static final String[] SERVERS = {"http://ec2-54-85-147-171.compute-1.amazonaws.com"};
//
//    private static final CloseableHttpClient client = CloseableHttpClientFactory.getInstance(25000, 25000, 1, 1);
//
//    @Override
//    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
//            throws ServletException, IOException {
//        String path = request.getRequestURI();
//        int serverNumber = 0;
//
//        if (path.startsWith("/q6")) {
//            final String tid = request.getParameter("tid");
//
//            serverNumber = tid.hashCode() % SERVERS.length;
//        } else {
//            serverNumber = path.hashCode() % SERVERS.length;
//        }
//
//        StringBuilder getUrlBuilder = new StringBuilder().append(SERVERS[serverNumber]).append(path).append("?").append(request.getQueryString());
//        HttpGet httpGet = new HttpGet(getUrlBuilder.toString());
//
//        HttpResponse httpResponse = client.execute(httpGet);
//        String inputLine;
//        StringBuffer serverResponse = new StringBuffer();
//
//        try (BufferedReader rd = new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent()))) {
//            while ((inputLine = rd.readLine()) != null) {
//                serverResponse.append(inputLine).append("\n");
//            }
//        } catch (IOException ex) {}
//
//        response.setHeader("Content-Type", "text/plain; charset=UTF-8");
//        response.getOutputStream().write(serverResponse.toString().getBytes());
//    }
//
//}
