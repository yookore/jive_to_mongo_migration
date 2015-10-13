package com.yookos.profile.migration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import com.rabbitmq.client.Channel;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import static com.yookos.profile.migration.utils.PostgresUtils.*;
import static com.yookos.profile.migration.utils.RabbitMqUtils.*;

public class Publisher implements Callable<Boolean> {

	//
	private  static final String UAA = "jdbc:postgresql://10.10.10.227:5432/uaa";
	private  static final String UAA_PASSWORD = "postgres";
	private  static final String UAA_USERNAME = "postgres";
	private  static final String QUEUE_NAME = "jiveprofile_queue";
	private  static final String EXCHANGE_NAME = "jiveprofile_exchange";
	
	//
	private  static final String RABBIT_MQ_HOST = "192.168.10.29";
	private  static final int RABBIT_MQ_PORT = 5672;
	private  static final String RABBIT_MQ_USERNAME = "test";
	private  static final String RABBIT_MQ_PASSWORD = "Wordpass15";
	
	private int limit;
	private int offset;
	
	private static Connection connection;
	private static Statement statement;
	
	Subscriber subscriber = new Subscriber();
	
    public Publisher(BlockingQueue<Integer> queue, int batch, int proccessId) {
        
    	this.limit = batch;
    	try {
            this.offset = ((Integer)queue.take());
            System.out.println("Process ID " + proccessId + " offset " + offset + " (from item " + offset + " to item " + (offset + limit) + ")");
        } 
    	catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
            
    public void publish() {
		
		JSONArray users = new JSONArray();
		if (connection == null) {
			if (isPostgresDriverLoaded()) { 
				connection = openPostgresConnection(UAA, UAA_USERNAME, UAA_PASSWORD);
				try {
					statement = connection.createStatement();
				} 
				catch (SQLException e) {
					e.printStackTrace();
				}
			}
			else {
				throw new AssertionError("Failed to load POSTGRES Driver.");
			}
		}
		try {
	        ResultSet rs = statement.executeQuery(String.format("SELECT users.id, "
	        												    + "users.created AS creationdate, "
	        												    + "users.lastmodified AS modificationdate, "
	        												    + "users.username, "
	        												    + "users.email, "
	        												    + "users.givenname AS firstname, "
	        												    + "users.familyname AS lastname, "
	        												    + "users.active, "
	        												    + "users.phonenumber, "
	        												    + "legacyusers.userid AS jiveuserid FROM users, "
	        												    + "legacyusers"
	        												    + " WHERE CAST(legacyusers.yookoreid AS TEXT) = users.id  OFFSET %s LIMIT %s;", offset, limit));
	        while (rs.next()) {
	        	if (rs.getString("username") == null || rs.getString("email") == null) {
	        		continue;
	        	}
	        	JSONObject user = new JSONObject();
	        	user.put("creationdate", convert(rs.getDate("creationdate")));
	        	user.put("modificationdate", convert(rs.getDate("modificationdate")));
	        	user.put("username", rs.getString("username"));
	        	user.put("email", rs.getString("email"));
	        	user.put("firstname", (rs.getString("firstname") == null)? "" : rs.getString("firstname"));
	        	user.put("lastname", (rs.getString("lastname") == null)? null : rs.getString("lastname"));
	        	user.put("active", Boolean.toString(rs.getBoolean("active")));
	        	user.put("phonenumber", (rs.getString("phonenumber") == null)? "" : rs.getString("phonenumber"));
	        	user.put("id", rs.getString("id"));
	        	user.put("jiveuserid", rs.getString("jiveuserid"));
	        	users.add(user);
	        }
	        rs.close();
			//getChannel().basicPublish(EXCHANGE_NAME, "", null, (users.toJSONString()).getBytes());
	        
	        JSONParser parser = new JSONParser();
	        try {
				JSONArray array = (JSONArray)parser.parse(users.toJSONString());
				subscriber.process(array);
			} 
	        catch (ParseException e) {
				e.printStackTrace();
			}
		}    
		catch (SQLException e) {
			e.printStackTrace();
		} 
//		catch (IOException e) {
//			e.printStackTrace();
//		}
		offset = offset + limit;
        System.out.println("DONE === From item " + offset + " to item " + (offset + limit) + ")");
	}
    
    private String convert(Date date) {
    	
    	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	return dateFormat.format(date); 
    }

    private Channel getChannel() {
    	
    	try {
    		Channel channel = openRabbitMqConnection(RABBIT_MQ_HOST, RABBIT_MQ_PORT, RABBIT_MQ_USERNAME, RABBIT_MQ_PASSWORD).createChannel();
//    		channel.exchangeDeclare(EXCHANGE_NAME, "topic");
//    		channel.queueDeclare(QUEUE_NAME, true, false, false, null);
//    		channel.queueBind(EXCHANGE_NAME, EXCHANGE_NAME, "");
			return channel;
		} 
    	catch (IOException e) {
			e.printStackTrace();
		}
    	throw new AssertionError("");
    }
    
    public Boolean call() {
    	
    	publish();
    	return true;
    }
}
