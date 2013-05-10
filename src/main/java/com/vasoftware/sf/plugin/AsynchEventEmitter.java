/**
 * User: bmcconnell
 * Date: 3/10/13
 * Time: 12:12 PM
 */

package com.vasoftware.sf.plugin;

import java.util.HashMap;
import java.io.InputStream;
import java.util.Properties;
import java.util.Map;
import java.io.IOException;
import java.lang.reflect.Method;

import com.collabnet.ce.soap50.types.SoapFieldValues;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.pool.ObjectPool;
import com.rabbitmq.client.Channel;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.apache.commons.pool.impl.GenericObjectPool;
import com.google.gson.Gson;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.vasoftware.sf.events.EventHandler50;

import org.apache.commons.pool.impl.GenericObjectPoolFactory;

/*
This class emits tracker artifact events to a RabbitMQ message bus.
*/
public class AsynchEventEmitter extends EventHandler50 {

    public final static Log log = LogFactory.getLog(AsynchEventEmitter.class);
    private static final String TRACKER_EVENT_TYPE = "artifact";
    private static final String GET = "get";
    private static final String KEYS = "Keys";
    private static final String VALUES = "Values";
    private static final String NULL_TOKEN = "null";
    private static final String ORIGINAL_DATA = "originalData";
    private static final String UPDATED_DATA = "updatedData";
    private static final String FOLDER_ID = "FolderId";
    private static final String ID = "Id";
    private static final String TYPE = "Type";
    private static final String OPERATION = "Operation";
    private static final String USERNAME = "UserName";
    private static final String COMMENT = "Comment";
    private static final String PROJECTID = "ProjectId";
    private static final String TIME_OUTPUT = "AsynchEventEmitter: processEvent took %s";
    private static final String EXCHANGE_NAME = "sfevents";
    private static final String ROUTING_KEY = "%s.%s.%s.%s";
    //These need to be static so that they are only created once and access to them should be synchronized.
    private static final ConnectionFactory factory = new ConnectionFactory();
    private static ObjectPool pool;
    private static final String messageString = "AsynchEventEmitter:  ROUTING_KEY=[%s]; MESSAGE=[%s]";
    //RabbitMQ static variables
    private static final String RABBIT_SERVICE_LOCATION = "rabbitServiceLocation";  //Config File Variable
    private static final String RABBIT_SERVICE_PORT = "rabbitServicePort"; //Config File Variable
    private static final String RABBIT_SERVICE_USERNAME = "rabbitServiceUser";  //Config File Variable
    private static final String RABBIT_SERVICE_PASSWORD = "rabbitServicePassword";  //Config File Variable
    private static final String RABBIT_SERVICE_MAX_CONNECTIONS = "rabbitServiceMaxConnections";
    private static final String PROPERTIES_FILE = "META-INF/config.properties";  //Config File Location
    private static String defaultRabbitServiceLocation = "localhost";  //Default if none supplied
    private static int defaultRabbitServicePort = 8080;  //Default if none supplied
    private static String defaultRabbitServiceUser = "guest";  //Default if none supplied
    private static String defaultRabbitServicePassword = "guest";  //Default if none supplied
    private static int defaultRabbitServiceMax = 10;  //Default if none supplied


    private static ObjectPool initRabbitMQConnectionPool(Properties prop) throws IOException {
        try {
            String locationString = prop.getProperty(RABBIT_SERVICE_LOCATION);
            String portString = prop.getProperty(RABBIT_SERVICE_PORT);
            String userString = prop.getProperty(RABBIT_SERVICE_USERNAME);
            String passwordString = prop.getProperty(RABBIT_SERVICE_PASSWORD);
            String maxConnectString = prop.getProperty(RABBIT_SERVICE_MAX_CONNECTIONS);
            if (locationString != null) {
                defaultRabbitServiceLocation = locationString;
            }
            if (portString != null) {
                defaultRabbitServicePort = new Integer(portString).intValue();
            }
            if (userString != null) {
                defaultRabbitServiceUser = userString;
            }
            if (passwordString != null) {
                defaultRabbitServicePassword = passwordString;
            }
            if (maxConnectString != null) {
                defaultRabbitServiceMax = new Integer(maxConnectString).intValue();
            }
        } catch (Exception e) {
            log.error("AsynchEventEmitter:  While reading the properties file "+ PROPERTIES_FILE +
                    " an error occured", e);
        }
        log.info("AsynchEventEmitter: Factory Settings: defaultRabbitServiceLocation=" + defaultRabbitServiceLocation + "; defaultRabbitServicePort=" + defaultRabbitServicePort +
                "; defaultRabbitServiceUser=" + defaultRabbitServiceUser + "; defaultRabbitServicePassword= " + defaultRabbitServicePassword + "; defaultRabbitServiceMax=" + defaultRabbitServiceMax);
        factory.setUsername(defaultRabbitServiceUser);
        factory.setPassword(defaultRabbitServicePassword);
        factory.setHost(defaultRabbitServiceLocation);
        factory.setPort(defaultRabbitServicePort);
        Config config = new GenericObjectPool.Config();
        config.maxActive = defaultRabbitServiceMax;
        config.testOnBorrow = true;
        config.testWhileIdle = true;
        config.timeBetweenEvictionRunsMillis = 10000;
        config.minEvictableIdleTimeMillis = 60000;
        PoolableObjectFactory rabbitMQPoolableObjectFactory = new RabbitMQPoolableObjectFactory(factory);
        GenericObjectPoolFactory genericObjectPoolFactory = new GenericObjectPoolFactory(rabbitMQPoolableObjectFactory, config);
        return genericObjectPoolFactory.createPool();
    }

    public AsynchEventEmitter() {
        try {
            if (pool == null) {
                InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE);
                Properties prop = new Properties();
                prop.load(inputStream);
                synchronized (this) {
                    initRabbitMQConnectionPool(prop);
                }
            }
            return;
        } catch (IOException e) {
            log.error(e);
        }
    }

    private void sendMessage(String message, String key) {
        if (pool != null){
            try {
                log.info(String.format(messageString, key, message));
                Channel channel = (Channel)pool.borrowObject();
                channel.basicPublish(EXCHANGE_NAME, key, null, message.getBytes());
                pool.returnObject(channel);
            } catch(Exception ex) {
                log.info(ex);
                ex.printStackTrace();
            }
        }
        else
        {
            log.info("AsynchEventEmitter:  Connection Pool is null.  Restarting.");
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE);
            Properties prop = new Properties();
            try {
                prop.load(inputStream);
                initRabbitMQConnectionPool(prop);
            } catch (Exception e) {
                log.info("AsynchEventEmitter:  Failed to restart connection pool");
            }
        }
    }

    @Override
    public void processEvent() throws Exception {
        String type = getEventContext().getType();

        //Only process Artifact events.  This is NOT meant to be a general purpose EventHandler
        if (!type.equals(TRACKER_EVENT_TYPE)) {
            return;
        }
        //Don't start method timing unless we are supposed to be here
        long startTime = System.currentTimeMillis();
        String operation = getEventContext().getOperation();

        String projectId = getEventContext().getProjectId();
        String comment = getEventContext().getComment();
        String userName = getEventContext().getUsername();

        Object originalData = getOriginalData();
        Object updatedData = getUpdatedData();

        Map <String, Object> event = new HashMap<String, Object>();
        event.put(TYPE, type);
        event.put(OPERATION, operation);
        event.put(PROJECTID, projectId == null ? NULL_TOKEN : projectId);
        event.put(USERNAME, userName);
        event.put(COMMENT, comment == null ? NULL_TOKEN : comment);

        Class originalDataClass = getOriginalData().getClass();
        Method[] methods = originalDataClass.getMethods();
        Map <String, Object> original_data = new HashMap <String, Object>();
        for (Method m : methods) {
            // we are only interested in getter methods
            if (!m.getName().startsWith(GET)) {
                continue;
            }
            // we are only interested in methods that do not take any parameters
            if (m.getParameterTypes().length != 0) {
                continue;
            }
            // should we only invoke methods with a simple return type?
            Object result = m.invoke(originalData);
            Map<String, Object> original_flex = new HashMap<String, Object>();
            if (result instanceof SoapFieldValues){
                String[] names = ((SoapFieldValues) result).getNames();
                Object[] values = ((SoapFieldValues) result).getValues();
                String[] strValues = new String[values.length];
                for (int i = 0; i <= values.length - 1; i++){
                    strValues[i]=(values[i] == null ? null : values[i].toString());
                }
                original_flex.put(KEYS, names);
                original_flex.put(VALUES, strValues);
                original_data.put(m.getName().substring(3), result == null ? NULL_TOKEN : original_flex);
            } else {
                original_data.put(m.getName().substring(3), result == null ? NULL_TOKEN : result.toString());
            }
        }

        Class updatedDataClass = getUpdatedData().getClass();
        methods = updatedDataClass.getMethods();
        Map <String, Object> updated_data = new HashMap <String, Object>();
        for (Method m : methods) {
            // we are only interested in getter methods
            if (!m.getName().startsWith(GET)) {
                continue;
            }
            // we are only interested in methods that do not take any parameters
            if (m.getParameterTypes().length != 0) {
                continue;
            }
            // should we only invoke methods with a simple return type?
            Object result = m.invoke(updatedData);
            Map<String, Object> updated_flex = new HashMap<String, Object>();
            if (result instanceof SoapFieldValues){
                String[] names = ((SoapFieldValues) result).getNames();
                Object[] values = ((SoapFieldValues) result).getValues();
                String[] strValues = new String[values.length];
                for (int i = 0; i <= values.length - 1; i++){
                    strValues[i]=(values[i] == null ? null : values[i].toString());
                }
                updated_flex.put(KEYS, names);
                updated_flex.put(VALUES, strValues);
                updated_data.put(m.getName().substring(3), result == null ? NULL_TOKEN : updated_flex);
            } else {
                updated_data.put(m.getName().substring(3), result == null ? NULL_TOKEN : result.toString());
            }
        }
        Gson gson = new Gson();
        event.put(ORIGINAL_DATA, original_data);
        event.put(UPDATED_DATA, updated_data);
        String json = gson.toJson(event);

        //Build routing_key
        String routing_key = String.format(ROUTING_KEY, projectId, (String)original_data.get(FOLDER_ID), (String)original_data.get(ID), operation);
        this.sendMessage(json, routing_key);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        log.info(String.format(TIME_OUTPUT,Long.toString(duration)));
    }
}
