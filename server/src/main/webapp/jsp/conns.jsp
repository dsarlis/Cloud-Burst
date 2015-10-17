<%@ page import="java.io.PrintWriter" %>
<%@ page import="java.util.*" %>
<%@ page import="java.util.concurrent.ConcurrentHashMap" %>
<%@ page import="javax.management.remote.JMXServiceURL" %>
<%@ page import="javax.management.remote.JMXConnectorFactory" %>
<%@ page import="javax.management.remote.JMXConnector" %>
<%@ page import="javax.management.*" %>
<%!
    private static Map<String, Integer> counts = new ConcurrentHashMap<String, Integer>();
    private static Map<String, Long> times = new ConcurrentHashMap<String, Long>();
%><%

    JMXServiceURL serviceUrl = new JMXServiceURL("service:jmx:rmi://localhost:1099/jndi/rmi://localhost:1099/jmxrmi");

    JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceUrl);
    System.out.println("Connecting to: " + serviceUrl);

    MBeanServerConnection mbeanServerConnection = jmxConnector.getMBeanServerConnection();
    MBeanServer mBeanServer = MBeanServerFactory.findMBeanServer(null).get(0);


    Vector threadPools = new Vector();
    Vector globalRequestProcessors = new Vector();


    String onStr = "*:type=queuedthreadpool,*";
    ObjectName objectName = new ObjectName(onStr);
    Set set = mbeanServerConnection.queryMBeans(objectName, null);
    Iterator iterator = set.iterator();
    while (iterator.hasNext()) {
        ObjectInstance oi = (ObjectInstance) iterator.next();
        threadPools.addElement(oi.getObjectName());
    }

    // Query Global Request Processors
    onStr = "*:type=statisticshandler,*";
    objectName = new ObjectName(onStr);
    set = mbeanServerConnection.queryMBeans(objectName, null);
    iterator = set.iterator();
    while (iterator.hasNext()) {
        ObjectInstance oi = (ObjectInstance) iterator.next();
        globalRequestProcessors.addElement(oi.getObjectName());
    }


    PrintWriter writer = response.getWriter();
    Enumeration enumeration = threadPools.elements();
    while (enumeration.hasMoreElements()) {
        objectName = (ObjectName) enumeration.nextElement();
        String name = objectName.getKeyProperty("name");

        writer.print(objectName + "<br>\n");

        writer.print("Current Thread Count: ");
        writer.print(mbeanServerConnection.getAttribute(objectName, "threads"));
        writer.print("/ Idle Threads: ");
        writer.print(mbeanServerConnection.getAttribute(objectName, "idleThreads"));
        writer.print("/ Max threads: ");
        writer.print(mbeanServerConnection.getAttribute(objectName, "maxThreads"));
        writer.print("/");

    }

    writer.print(objectName + "<br>\n");

    ObjectName grpName = null;

    Enumeration e2 = globalRequestProcessors.elements();
    while (e2.hasMoreElements()) {
        ObjectName objectName2 = (ObjectName) e2.nextElement();
            grpName = objectName2;

        writer.print("Requests: ");
        writer.print(mbeanServerConnection.getAttribute(grpName, "requests"));


        int count = (Integer) mbeanServerConnection.getAttribute(grpName, "requests");

        synchronized (this) {

            long now = System.currentTimeMillis();

            String key = objectName.toString();
            int nCountDiff = (count - (counts.containsKey(key) ? counts.get(key) : 0));
            long nTimeDiff = (now - (times.containsKey(key) ? times.get(key) : 0));

            double qps = (nCountDiff / (double) nTimeDiff) * 1000;


            counts.put(key, count);
            times.put(key, System.currentTimeMillis());

            writer.print("/");
            writer.print("QPS: ");
            writer.print((int) qps);

        }

    }
%>
