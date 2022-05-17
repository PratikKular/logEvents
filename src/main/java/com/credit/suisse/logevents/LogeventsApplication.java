package com.credit.suisse.logevents;
 


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


 
 
public class LogeventsApplication {
	
	  private static final Logger log = LoggerFactory.getLogger(LogeventsApplication.class);
	public static void main(String[] args)   {
		
		
		try 
		{
		LogEventSerivce.logEvents(args[0]);	
	}
		catch(Exception e) {
			
			log.error("Error in Logevents "+ e.getMessage());
			
		}
	}

}
