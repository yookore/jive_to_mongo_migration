package com.yookos.profile.migration.utils;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public final class RabbitMqUtils {

	public static ConnectionFactory factory;
    
    public static Connection openRabbitMqConnection(String host, int port, String username, String password) {
    
        Connection connection = null;
		
        try {
			factory = new ConnectionFactory();
	        factory.setHost(host);
	        //factory.setVirtualHost("jiveprofile");
	        factory.setUsername(username);
	        factory.setPassword(password);
			connection = factory.newConnection();
		} 
		catch (IOException e) {
			e.printStackTrace();
		} 
		catch (TimeoutException e) {
			e.printStackTrace();
		}
    	return connection;
    }
}
