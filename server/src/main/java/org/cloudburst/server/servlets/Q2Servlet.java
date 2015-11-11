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
 * Servlet for Q2.
 */
public class Q2Servlet extends HttpServlet {

	private MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());


	private static Map<String, String> cache = new HashMap<String, String>();
	private static String FIRST_LINE;

	public static void setFirstLine(String teamId, String teamAWSId) {
		FIRST_LINE = teamId + "," + teamAWSId + "\n";
	}

	@Override
	public void init(ServletConfig servletConfig) throws ServletException {
		super.init(servletConfig);
//		initMySqlService();
	}

	/**
	 * Initialize MySQL connection pool.
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
		String key = userId + creationTime;
		String result = cache.get(key);

		if (result == null) {
			StringBuilder finalMessage = new StringBuilder(FIRST_LINE);

			finalMessage.append(mySQLService.getTweetResult(userId, creationTime));
			result = finalMessage.toString();
			if (cache.size() < 1000000) cache.put(key, result);
		}
		response.setHeader("Content-Type", "text/plain; charset=UTF-8");
		response.getOutputStream().write(result.getBytes());
	}

}
