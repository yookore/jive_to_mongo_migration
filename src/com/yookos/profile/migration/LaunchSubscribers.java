package com.yookos.profile.migration;

import static com.yookos.profile.migration.utils.RabbitMqUtils.openRabbitMqConnection;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class LaunchSubscribers {
	
	//
	private  static final String QUEUE_NAME = "jiveprofile_queue";
	private  static final String RABBIT_MQ_HOST = "192.168.10.29";
	private  static final int RABBIT_MQ_PORT = 5672;
	private  static final String RABBIT_MQ_USERNAME = "test";
	private  static final String RABBIT_MQ_PASSWORD = "Wordpass15";
		
	public static Channel getChannel() {
    	
    	try {
    		Channel channel = openRabbitMqConnection(RABBIT_MQ_HOST, RABBIT_MQ_PORT, RABBIT_MQ_USERNAME, RABBIT_MQ_PASSWORD).createChannel();
    		channel.queueDeclare(QUEUE_NAME, true, false, false, null);
			return channel;
		} 
    	catch (IOException e) {
			e.printStackTrace();
		}
    	throw new AssertionError("");
    }
	
	public static void main(String[] args) {
		
		Channel channel = getChannel();
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
				
				String message = new String(body, "UTF-8");
		        JSONParser parser = new JSONParser();
		        try {
					JSONArray array = (JSONArray)parser.parse(message);
					new Subscriber(array);
				} 
		        catch (ParseException e) {
					e.printStackTrace();
				}
			}
		};
		try {
			channel.basicConsume(QUEUE_NAME, true, consumer);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
