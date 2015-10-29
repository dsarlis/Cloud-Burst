package org.cloudburst.server.servlets;

import org.cloudburst.server.services.CipherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Servlet to answer Q1.
 */
public class Q1Servlet extends HttpServlet {

    private final static Logger logger = LoggerFactory.getLogger(Q1Servlet.class);
    private CipherService cipherService = new CipherService();
    private static String FIRST_LINE;

    public static void setFirstLine(String teamId, String teamAWSId) {
        FIRST_LINE = teamId + "," + teamAWSId + "\n";
    }

    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);
    }

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        String key = request.getParameter("key");
        String message = request.getParameter("message");

        String decryptedMessage = cipherService.decrypt(key, message);
        StringBuilder finalMessage = new StringBuilder();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        finalMessage.append(FIRST_LINE);
        finalMessage.append(simpleDateFormat.format(Calendar.getInstance().getTime())).append("\n");
        finalMessage.append(decryptedMessage).append("\n");

        response.addHeader("Connection", "keep-alive");
        response.getOutputStream().write(finalMessage.toString().getBytes());
    }

}
