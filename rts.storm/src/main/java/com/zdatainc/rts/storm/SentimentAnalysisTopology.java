package com.zdatainc.rts.storm;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.StringScheme;

public class SentimentAnalysisTopology
{
    private final Logger LOGGER = Logger.getLogger(this.getClass());
    private static final String KAFKA_TOPIC =
            Properties.getString("rts.storm.kafka_topic");

    public static void main(String[] args) throws Exception
    {
        BasicConfigurator.configure();

        if (args != null && args.length > 0)
        {
            StormSubmitter.submitTopology(
                    args[0],
                    createConfig(false),
                    createTopology());
        }
        else
        {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(
                    "sentiment-analysis",
                    createConfig(true),
                    createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }

    private static StormTopology createTopology()
    {
        int numberOfExecutors = 1; //original 4;
        SpoutConfig kafkaConf = new SpoutConfig(
                new ZkHosts(Properties.getString("rts.storm.zkhosts")),
                KAFKA_TOPIC,
                "/kafka",
                "KafkaSpout");
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder topology = new TopologyBuilder();

        topology.setSpout("kafka_spout", new KafkaSpout(kafkaConf), numberOfExecutors);

        topology.setBolt("twitter_filter", new TwitterFilterBolt(), numberOfExecutors)
                .shuffleGrouping("kafka_spout");

        topology.setBolt("text_filter", new TextFilterBolt(), numberOfExecutors)
                .shuffleGrouping("twitter_filter");

        topology.setBolt("stemming", new StemmingBolt(), numberOfExecutors)
                .shuffleGrouping("text_filter");

        topology.setBolt("positive", new PositiveSentimentBolt(), numberOfExecutors)
                .shuffleGrouping("stemming");
        topology.setBolt("negative", new NegativeSentimentBolt(), numberOfExecutors)
                .shuffleGrouping("stemming");

        topology.setBolt("join", new JoinSentimentsBolt(), numberOfExecutors)
                .fieldsGrouping("positive", new Fields("tweet_id"))
                .fieldsGrouping("negative", new Fields("tweet_id"));

        topology.setBolt("score", new SentimentScoringBolt(), numberOfExecutors)
                .shuffleGrouping("join");

        topology.setBolt("hdfs", new HDFSBolt(), numberOfExecutors)
                .shuffleGrouping("score");

        topology.setBolt("nodejs", new NodeNotifierBolt(), numberOfExecutors)
                .shuffleGrouping("score");

        return topology.createTopology();
    }

    private static Config createConfig(boolean local)
    {
        int workers = Properties.getInt("rts.storm.workers");
        Config conf = new Config();
        conf.setDebug(true);
        if (local)
            conf.setMaxTaskParallelism(workers);
        else
            conf.setNumWorkers(workers);
        return conf;
    }
}

