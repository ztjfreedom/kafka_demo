package com.itcast.kafkademo.chapter10;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

public class JmxConnectionDemo {
    private MBeanServerConnection conn;
    private String jmxURL;
    private String ipAndPort;

    public static void main(String[] args) {
        JmxConnectionDemo jmxConnectionDemo = new JmxConnectionDemo("localhost:9999");
        jmxConnectionDemo.init();
        System.out.println(jmxConnectionDemo.getMsgInPerSec());
    }

    public JmxConnectionDemo(String ipAndPort) {
        this.ipAndPort = ipAndPort;
    }

    public boolean init() {
        jmxURL = "service:jmx:rmi:///jndi/rmi://" + ipAndPort + "/jmxrmi";
        try {
            JMXServiceURL serviceURL = new JMXServiceURL(jmxURL);
            JMXConnector connector = JMXConnectorFactory.connect(serviceURL, null);
            conn = connector.getMBeanServerConnection();
            if (conn == null) {
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public double getMsgInPerSec() {
        String objectName = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec";
        Object val = getAttribute(objectName, "OneMinuteRate");
        if (val != null) {
            return (double) (Double) val;
        }
        return 0.0;
    }

    private Object getAttribute(String objName, String objAttr) {
        ObjectName objectName;
        try {
            objectName = new ObjectName(objName);
            return conn.getAttribute(objectName, objAttr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}