package com.storm.twitter;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import twitter4j.internal.logging.Logger;
import twitter4j.DirectMessage;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.UserList;
import twitter4j.UserStreamListener;
import twitter4j.conf.ConfigurationBuilder;


public class TwitterSpout implements IRichSpout {

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;
	private TwitterStream twitterStream;
	
	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		 twitterStream.shutdown();
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
    public void nextTuple() {
        // Status object from the queue.
        Status status = queue.poll();
        if(status!=null)
        System.out.println("Tuple::"+status);
        if(status == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(status));
        }
    }

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		queue= new LinkedBlockingQueue<Status>();
		
		UserStreamListener listener = new UserStreamListener() {
			 @Override
	            public void onException(Exception arg0) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onTrackLimitationNotice(int arg0) {
	                // TODO Auto-generated method stub
	 
	            }
	            // This method executes every time a new tweet comes in.
	            @Override
	            public void onStatus(Status status) {
	                queue.offer(status);
	            }
	 
	            @Override
	            public void onStallWarning(StallWarning arg0) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onScrubGeo(long arg0, long arg1) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onDeletionNotice(StatusDeletionNotice arg0) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onUserProfileUpdate(User arg0) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onUserListUpdate(User arg0, UserList arg1) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onUserListUnsubscription(User arg0, User arg1, UserList arg2) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onUserListSubscription(User arg0, User arg1, UserList arg2) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onUserListMemberDeletion(User arg0, User arg1, UserList arg2) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onUserListMemberAddition(User arg0, User arg1, UserList arg2) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onUserListDeletion(User arg0, UserList arg1) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onUserListCreation(User arg0, UserList arg1) {
	                // TODO Auto-generated method stub
	 
	            }
	 
//	            @Override
//	            public void onUnfollow(User arg0, User arg1) {
//	                // TODO Auto-generated method stub
	// 
//	            }
	 
	            @Override
	            public void onUnfavorite(User arg0, User arg1, Status arg2) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onUnblock(User arg0, User arg1) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onFriendList(long[] arg0) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onFollow(User arg0, User arg1) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onFavorite(User arg0, User arg1, Status arg2) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onDirectMessage(DirectMessage arg0) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onDeletionNotice(long arg0, long arg1) {
	                // TODO Auto-generated method stub
	 
	            }
	 
	            @Override
	            public void onBlock(User arg0, User arg1) {
	                // TODO Auto-generated method stub
	 
	            }
	        };
	        ConfigurationBuilder cb = new ConfigurationBuilder();
	        cb.setOAuthConsumerKey(ApplicationConstants.CONSUMER_KEY);
	        cb.setOAuthConsumerSecret(ApplicationConstants.CONSUMER_SECRET_KEY);
	        cb.setOAuthAccessToken(ApplicationConstants.ACCESS_TOKEN_KEY);
	        cb.setOAuthAccessTokenSecret(ApplicationConstants.ACCESS_TOKEN_SECRET_KEY);
	        
	        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
	        twitterStream.addListener(listener);
	        twitterStream.user();
	 
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("status"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
