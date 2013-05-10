/**
 * User: emcconne
 * Date: 3/10/13
 * Time: 12:12 PM
 */

package com.vasoftware.sf.plugin;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.pool.BasePoolableObjectFactory;

public class RabbitMQPoolableObjectFactory extends BasePoolableObjectFactory {
    private ConnectionFactory factory;

    public RabbitMQPoolableObjectFactory(ConnectionFactory factory) {
        this.factory = factory;
    }

    @Override
    public Object makeObject() throws Exception {
        Connection connection = this.factory.newConnection();
        return connection.createChannel();
    }
}
