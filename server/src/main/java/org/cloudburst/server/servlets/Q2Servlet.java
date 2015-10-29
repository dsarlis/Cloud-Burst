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

public class Q2Servlet extends HttpServlet {

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
	 * Initialize MySQL connection pooll.
	 */
	private void initMySqlService() {
		Properties boneCPConfigProperties = new Properties();
		try {
			boneCPConfigProperties.load(Q2Servlet.class.getResourceAsStream("/bonecp.properties"));
		} catch (IOException ex) {}

		MySQLConnectionFactory.init(boneCPConfigProperties);
	}

	@Override
	public void doGet(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {

		long userId = Long.valueOf(request.getParameter("userid"));
		String creationTime = request.getParameter("tweet_time");

		StringBuilder finalMessage = new StringBuilder(FIRST_LINE);
		finalMessage.append(mySQLService.getTweetResult(userId, creationTime));

		response.getOutputStream().write(finalMessage.toString().getBytes());
	}

}
