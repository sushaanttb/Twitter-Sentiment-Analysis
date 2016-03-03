package com.storm.twitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class MainToplogy {

	public static void main(String [] args) throws Exception
	{
	Config config= new Config();
	config.setDebug(true);
	config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
	
	TopologyBuilder builder = new TopologyBuilder();
	builder.setSpout("twitter-spout", new TwitterSpout());
	builder.setBolt("details-extractor", new DetailsExtractorBolt()).shuffleGrouping("twitter-spout");
	
	LocalCluster cluster = new LocalCluster();
	cluster.submitTopology("HelloTwitter", config, builder.createTopology());
	
	 Runtime.getRuntime().addShutdownHook(new Thread()    {
         @Override
         public void run()    {
         cluster.killTopology(ApplicationConstants.TOPOLOGY_NAME);
         cluster.shutdown();}
     });
 }
}
