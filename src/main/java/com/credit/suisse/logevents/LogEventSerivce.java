package com.credit.suisse.logevents;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.credit.suisse.logevents.ModelJson; 
 
public class LogEventSerivce {
	
	  private static final Logger log = LoggerFactory.getLogger(LogEventSerivce.class);
	
	final static long noOfLinesforThreads =  200000L;//Logic to decide  Threads per N number of lines
	final static int maxThread =  20;  //No of Threads should not exceeds 20

	public static List<ModelJson> logEvents(String fileName) throws IOException, InterruptedException, ExecutionException {
		
		
		long startTime = System.currentTimeMillis();
		long endTime  ;
	
	 
		
		 Path filePath = Paths.get(fileName);
		  long numOfLines =   Files.lines(filePath ).count();
	
		  Set<Callable<List<ModelJson>>> callables = new HashSet<Callable<List<ModelJson>>>(); 
		  
		  Vector<ModelJson> mergedAllList = new Vector<ModelJson>();
		  
		  double logicalThreadCount =   (double) numOfLines/(double )noOfLinesforThreads ;
		  
 		  
		  log.info("numberOfThreadsTobeCreated "+Math.ceil(logicalThreadCount));  
		  
		  int actualthreadCount =     (int) (Math.ceil(logicalThreadCount) > maxThread ? maxThread: Math.ceil(logicalThreadCount));
		  
		  log.info("actual Threads "+actualthreadCount);  
		  
		  ExecutorService executor = Executors.newFixedThreadPool(actualthreadCount);// MAx Thread 20
		  
		 //Reading Different Part of files in Threads
		  for (int i  =0; i < Math.ceil(logicalThreadCount); i ++)
		
		  {final int count = i;
			  callables.add(new Callable<List<ModelJson>>() {  
		            public List<ModelJson> call() throws Exception {  
		            	List<ModelJson> listDataFromFile  = new LinkedList<ModelJson>() ;
		            	List<String> line;
		            	 log.info("Started Thread  "+ count);  
		            	try (BufferedReader reader = Files.newBufferedReader(
		    			        Paths.get(fileName), StandardCharsets.UTF_8))
		            	{
		            	  line = reader.lines()
	                              .skip((count*noOfLinesforThreads))
	                              .limit(noOfLinesforThreads)
	                              .collect(Collectors.toList());
		            	}
		            	 
		           	 log.debug("Ended Reading Thread  "+count); 
		            	    line.stream().forEach(data -> {  
		            	    	log.info("Started data -"+ data);  
		            	    	ModelJson modelJson = new ModelJson();
		            	    	JSONObject jsonObject = new JSONObject(data);
		            	    	modelJson.setId( jsonObject.getString("id"));
		            	    	String type;
		            	    	String host;
		            	    	try {
		            	    		type = jsonObject.getString("type");
		            	    		
		            	    	}
		            	    	catch(Exception e)
		            	    	{
		            	    		type = "";
		            	    		
		            	    	}
		            	    	modelJson.setType(type);
		            	    	try {
		            	    		host = jsonObject.getString("host");
		            	    		
		            	    	}
		            	    	catch(Exception e)
		            	    	{
		            	    		host = "";
		            	    		
		            	    	}
		            	    	
		            	    	modelJson.setHost(host);
		            	    	modelJson.setState(jsonObject.getString("state"));
		            	    	modelJson.setTimestamp(jsonObject.getLong("timestamp"));
		            	    	listDataFromFile.add(modelJson);
		            	    			            	 
		            	   });
		            		 log.info("Ended Thread -"+ count+ " size "+ listDataFromFile.size());  
		            	
		            	    return listDataFromFile;
		            }  
		        });
			  
		  }
		 		  
		  
 		 
		  
		  java.util.List<Future<List<ModelJson>>> futures = executor.invokeAll(callables);  
		  
		  
		  //Consolidating data from all threads
		    for(Future<List<ModelJson>> future : futures){  
	             
	            mergedAllList.addAll( future.get());
	              
	        }  
	  
		  
		    executor.shutdown();  
	
		    
		    //Data with Duplicate IDs will have the value as time difference in it.Data where FInisied  Event is not logged will have the value as Timestamps
		   Map<String, Long> finalMap = mergedAllList.stream().collect(
	                Collectors.toMap(ModelJson::getId, ModelJson::getTimestamp,
	                        (oldValue, newValue) ->(  Math.abs(newValue-oldValue ))
	                )
	        ); 
		   
		   
		   //Sorting the Map in asceding Order so get the timedifferences first (since only  Timestamp will have large value) 
		   LinkedHashMap<Object, Object> sorted = finalMap.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect( Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2, LinkedHashMap::new));


		   List<ModelJson> listfoundEvengts  = new LinkedList<ModelJson>() ;
		   
		   for (Map.Entry<Object, Object> m : sorted.entrySet())
		   {  
			   long  maxDiff = (long) m.getValue() ;
			   
			   if (maxDiff > 10000000000L)
			
				   {break;
				   }
			   else 
			   {
				   if (maxDiff >= 4)
				   {
					    log.info("Event with More Than 4ms diff " + m.getKey()); 
					    listfoundEvengts.addAll( mergedAllList.stream().filter(p -> p.getId().equalsIgnoreCase((String) m.getKey())).collect(Collectors.toList()) );
					    
				   }
			   }
				   
			   
			   
		   }
		  
			  endTime   = System.currentTimeMillis();
			  log.info("Total Time to Execute "+(endTime - startTime) + " milliseconds");  
		    
	       log.info("Records with Flag event to be Inserted in DB  "+listfoundEvengts);  
	       return listfoundEvengts;
		  }

}
