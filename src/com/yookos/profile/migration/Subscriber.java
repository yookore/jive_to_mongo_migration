package com.yookos.profile.migration;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;
import com.rabbitmq.client.Channel;

import static com.yookos.profile.migration.utils.PostgresUtils.*;
import static com.yookos.profile.migration.utils.RabbitMqUtils.openRabbitMqConnection;

public class Subscriber  {
	
	//
	private static final String HOST = "jdbc:postgresql://192.168.10.225:5432/yookos"; 
	private static final String DATABASE = "yookos"; 
	private static final String USERNAME = "postgres"; 
	private static final String PASSWORD = "postgres"; 
	//
	private static final String PROFILE_HOST = "10.10.10.216"; 
	private static final String PROFILE_DATABSE = "jiveuserprofile";
	private static final String PROFILE_COLLECTION = "userprofile";
	private static final String PROFILE_USERNAME = ""; 
	private static final String PROFILE_PASSWORD = "";
	private Mongo mongo;
	
	private static Connection connection;
	private static Statement statement;
	
	public Subscriber(JSONArray users) {
		
		transform(users);
	}	
	
	private List<String> transform(JSONArray users) {
		
		if (connection == null) {
			if (isPostgresDriverLoaded()) { 
				connection = openPostgresConnection(HOST, USERNAME, PASSWORD);
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
		for (int i = 0; i < users.size(); i++) {
			try {
				JSONObject user = (JSONObject)users.get(i);
				user.put("title", "");
				user.put("biography", ""); 
				user.put("gender", "");
				user.put("country", "");
				user.put("hobbies", "");
				user.put("birthdate", "");
				user.put("relationshipstatus", "");
				
				//contacts
				JSONObject contacts = new JSONObject();
				contacts.put("phonenumber", user.get("phonenumber"));
				contacts.put("mobilenumber", "");
				contacts.put("homenumber", "");
				contacts.put("email", user.get("email"));
				contacts.put("altemail", "");
				
				user.remove("phonenumber");
				user.remove("email");
				
				//work
				JSONObject work = new JSONObject();
				work.put("company", "");
				work.put("department", "");
				work.put("jobtitle", "");
				work.put("startdate", "");
				work.put("expertise", "");
				
				user.put("contacts", contacts);
				user.put("work", work);
				
		        ResultSet rs = statement.executeQuery(String.format("SELECT fieldid, value FROM jiveuserprofile WHERE userid = %s", user.get("jiveuserid")));
		        while (rs.next()) {
		        	int field = rs.getInt("fieldid");
		        	switch (field) {
			        	case 1:
			        		if (rs.getString("value") != null) {
			        			user.put("title", rs.getString("value"));
			        		}
			        		break;
			        	case 2:
			        		if (rs.getString("value") != null) {
			        			JSONObject department = (JSONObject)user.get("work");
			        			department.put("department", rs.getString("value"));
			        			user.put("work", department);
			        		}
			        		break;
			        	case 5:
			        		if (rs.getString("value") != null) {
			        			JSONObject homenumber = (JSONObject)user.get("contacts");
			        			homenumber.put("homenumber", rs.getString("value"));
			        			user.put("contacts", homenumber);
			        		}
			        		break;
			        	case 6:
			        		if (rs.getString("value") != null) {
			        			JSONObject mobilenumber = (JSONObject)user.get("contacts");
			        			mobilenumber.put("mobilenumber", rs.getString("value"));
			        			user.put("contacts", mobilenumber);
			        		}
			        		break;
			        	case 7:
			        		if (rs.getString("value") != null) {
			        			JSONObject startdate = (JSONObject)user.get("work");
			        			startdate.put("startdate", rs.getString("value"));
			        			user.put("work", startdate);
			        		}
			        		break;
			        	case 8:
			        		if (rs.getString("value") != null) {
			        			user.put("biography", rs.getString("value")); 
			        		}
			        		break;
			        	case 9:
			        		if (rs.getString("value") != null) {
			        			JSONObject expertise = (JSONObject)user.get("work");
			        			expertise.put("expertise", rs.getString("value"));
			        			user.put("work", expertise);
			        		}
			        		break;
			        	case 10:
			        		if (rs.getString("value") != null) {
			        			JSONObject altemail = (JSONObject)user.get("contacts");
			        			altemail.put("altemail", rs.getString("value"));
			        			user.put("contacts", altemail);
			        		}
			        		break;
			        	case 5001:
			        		if (rs.getString("value") != null) {
			        			user.put("gender", rs.getString("value"));
			        		}
			        		break;
			        	case 5006:
			        		if (rs.getString("value") != null) {
			        			user.put("title", rs.getString("value"));
			        		}
			        		break;	
			        	case 5009:
			        		if (rs.getString("value") != null) {
			        			user.put("country", rs.getString("value"));
			        		}
			        		break;
			        	case 5010:
			        		if (rs.getString("value") != null) {
			        			user.put("hobbies", rs.getString("value"));
			        		}
			        		break;
			        	case 5012:
			        		if (rs.getString("value") != null) {
			        			user.put("relationshipstatus", rs.getString("value"));
			        		}
			        		break;
			        	case 5015:
			        		if (rs.getString("value") != null) {
			        			JSONObject company = (JSONObject)user.get("work");
			        			company.put("company", rs.getString("value"));
			        			user.put("work", company);
			        		}
			        		break;
			        	case 5019:
			        		if (rs.getString("value") != null) {
			        			JSONObject jobtitle = (JSONObject)user.get("work");
			        			jobtitle.put("jobtitle", rs.getString("value"));
			        			user.put("work", jobtitle);
			        		}
			        		break;
			        	default:
			        		break;
		        	}
		        }
	        	load((DBObject)JSON.parse(user.toJSONString()));
		        rs.close();
			}    
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
		try {
			statement.close();
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private void load(DBObject document) {
		
		DB db = openMongoConnection(PROFILE_HOST, PROFILE_DATABSE, 27017, "", "");
		DBCollection collection = db.getCollection(PROFILE_COLLECTION);
		collection.insert(document);
	}
	
	private DB openMongoConnection(String url, String database, int port, String username, String password) {
	
		Mongo mongo;
		try {
			mongo = new Mongo(url, port);
			return mongo.getDB(database);
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		throw new AssertionError("");
	}
}
