package com.storm.twitter;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import com.storm.twitter.ApplicationConstants;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.net.URLConnection;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import twitter4j.Status;


public class DetailsExtractorBolt implements IRichBolt {

	OutputCollector collector;
	
	String text;
	String user;
	int words;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		Status status= (Status) tuple.getValueByField("status");
		
		text=status.getText();
		user=status.getUser().getName();
		System.out.println("New JSON Tweet Received::"+status);
		
		System.out.println("User::::"+user);
		System.out.println("Tweet Received at::::"+status.getCreatedAt().toString());
	
		String[] wc = (text).split("\\s");
		words=wc.length;
		
		//1)To show Original Tweet
		System.out.println("text::::"+text+" ("+words+" words)");
		
		
		//2)To Remove Stop Words

//		text=text.replaceAll("(?i)"+Pattern.quote("stop"), "\b");
//		text=text.replaceAll("(?i)"+Pattern.quote("stop"), "");
//		System.out.println("text without Stop Words(if any)::::"+text);
		
		text=removeStopWords(text);
		System.out.println("text without Stop Words(if any)::::"+text);
		
		//3) To Enrich Tweet With GeoLocation + Words Count
		if(status.getUser().isGeoEnabled())
	    {
			if(status.getPlace()!=null && status.getPlace().getFullName()!=null)
			{System.out.println("Location::::"+status.getPlace().getFullName());
	    text+=" From location:"+status.getPlace().getFullName();
			}
	    wc = (text).split("\\s");
		words=wc.length;
		System.out.println("Enriched text::::"+text+" ("+words+" words)");
	    }
	
			
		//4)Intent
		if(Pattern.compile(Pattern.quote("where"), Pattern.CASE_INSENSITIVE).matcher(text).find())
		System.out.println("Tweet Intent1::"+ApplicationConstants.BUYING_INTENT);
		
		if(Pattern.compile(Pattern.quote("what"), Pattern.CASE_INSENSITIVE).matcher(text).find())
		System.out.println("Tweet Intent2::"+ApplicationConstants.REQUEST_INTENT);
		
		//5)Sentiments
		int polarity=0;
		try {
			polarity = getSentimentResponse(text);
		} catch (UnsupportedEncodingException e) {
			
			e.printStackTrace();
		}
		
		if(polarity==4)
			System.out.println("Tweet Sentiment:"+ApplicationConstants.POSITIVE);
		if(polarity==2)
			System.out.println("Tweet Sentiment:"+ApplicationConstants.NEUTRAL);
		if(polarity==0)
			System.out.println("Tweet Sentiment:"+ApplicationConstants.NEGATIVE);
			
		
		collector.ack(tuple);
		
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public int getSentimentResponse(String text) throws UnsupportedEncodingException
	{
		HttpURLConnection connection = null;
		int polarity=0;
		StringBuilder response= new StringBuilder();
		StringBuilder targetURL= new StringBuilder();
		
		targetURL.append(ApplicationConstants.SENTIMENT_URL+ URLEncoder.encode(text, "UTF-8"));
				
		System.out.println("targetURL::"+targetURL.toString());
		
				try{
					
					URL url= new URL(targetURL.toString());
		
					connection = (HttpURLConnection) url.openConnection();
					connection.setRequestMethod("GET");
		
//					System.out.println("connection::"+connection);
		
					connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
					connection.setUseCaches(false);
					    
					  
					    //Get Response
					    InputStream is = connection.getInputStream();
					    BufferedReader rd = new BufferedReader(new InputStreamReader(is));
					     
					    String line;
					    while((line = rd.readLine()) != null) {
					      response.append(line);
					      response.append('\r');
					    }
					    rd.close();
					    
						System.out.println("JSON tweet sentiments:"+response.toString());

						int idx=response.indexOf("polarity");
//						System.out.println("Index of polarity:"+idx);
						polarity=Integer.parseInt(response.substring(idx+10,idx+11));
						System.out.println("Polarity Value:"+polarity);
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				finally
				{
					if(connection != null) {
					      connection.disconnect(); 
					    }
				}

				return polarity;
		}
	
	public String removeStopWords(String text)
	{
		StringBuilder retext= new StringBuilder();
		int k=0;
		ArrayList<String> wordsList = new ArrayList<String>();
		String sCurrentLine;
		String[] stopwords = new String[1000];
		try{
				//Populating Stop Words List
		        FileReader fr=new FileReader("stopwords.txt");
		        BufferedReader br= new BufferedReader(fr);
		        while ((sCurrentLine = br.readLine()) != null){
		            stopwords[k]=sCurrentLine;
		            k++;
		        }
		        //Populating Tweets Words List
		        StringBuilder sb = new StringBuilder(text);
		        String[] words = sb.toString().split("\\s");
		        for (String word : words){
		            wordsList.add(word);
		        }
		        
		        for(int i = 0; i< wordsList.size(); i++){
		            for(int j = 0; j< k; j++){
		                if(stopwords[j].contains(wordsList.get(i).toLowerCase())){
		                    wordsList.set(i,"");
		                    break;
		                }
		             }
		        }
		        for (String str : wordsList)
		            retext.append(str+" ");
		        	
		    }
			catch(Exception ex){
		        System.out.println(ex);
		    }
		
		return retext.toString().trim();
        
	}

}
