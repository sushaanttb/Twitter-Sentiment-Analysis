package com.storm.twitter;
public class ApplicationConstants {

public static final String CONSUMER_KEY="A33YatTuqHx0zzZc0C6JsVh76";
public static final String CONSUMER_SECRET_KEY="YlWRUviwqTnu5l4xuH7HTeo0nkfxoADZTCyY3HnwDzR9kdUwok";
public static final String ACCESS_TOKEN_KEY="55786112-f3bHBCOUT047brtNGmpPJZsGGn1peNOS33Ata77dh";
public static final String ACCESS_TOKEN_SECRET_KEY="LYzuSwhLGK3RdPbvGme0NFIOBw0dUkoYGblMmprxfqRZb";

// constants
public static final String NOT_AVAILABLE = "Not Available";
public static final String EMPTY = "";

// Topology Constants 
public static final String TOPOLOGY_NAME = "twitter-topology";
public static final String TWITTER_SPOUT_ID = "twitterSpout";
public static final String DETAILS_BOLT_ID = "detailsExtracterBolt";
public static final String RETWEET_DETAILS_BOLT_ID = "retweetDetailsExtracterBolt";
public static final String FILE_WRITER_BOLT_ID = "fileWriterBolt";

public static final String BUYING_INTENT="Buying Intent";
public static final String REQUEST_INTENT="Request Intent";

public static final String SENTIMENT_URL="http://www.sentiment140.com/api/classify?text=";
public static final String POSITIVE="Positive";
public static final String NEUTRAL="Neutral";
public static final String NEGATIVE="Negative";

}
