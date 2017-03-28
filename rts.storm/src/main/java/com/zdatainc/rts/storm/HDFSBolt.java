package com.zdatainc.rts.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HDFSBolt extends BaseRichBolt
{
    private static final long serialVersionUID = 42L;
    private static final Logger LOGGER = Logger.getLogger(HDFSBolt.class);
    private OutputCollector collector;
    private int id;
    private List<String> tweet_scores;
    private int batchSize = Properties.getInt("hadoop.batch.size");
    Configuration conf = null;

    @SuppressWarnings("rawtypes")
    public void prepare(
            Map stormConf,
            TopologyContext context,
            OutputCollector collector)
    {
        this.id = context.getThisTaskId();
        this.collector = collector;
        this.tweet_scores = new ArrayList<String>(batchSize);
        this.conf = getHdfsConfiguration();
    }

    public void execute(Tuple input)
    {
        Long id = input.getLong(input.fieldIndex("tweet_id"));
        String tweet = input.getString(input.fieldIndex("tweet_text"));
        Float pos = input.getFloat(input.fieldIndex("pos_score"));
        Float neg = input.getFloat(input.fieldIndex("neg_score"));
        String score = input.getString(input.fieldIndex("score"));
        String tweet_score =
                String.format("%s,%s,%f,%f,%s\n", id, tweet, pos, neg, score);
        this.tweet_scores.add(tweet_score);
        if (this.tweet_scores.size() >= batchSize)
        {
            writeToHDFS();
            this.tweet_scores = new ArrayList<String>(batchSize);
        }
    }

    private void writeToHDFS()
    {
        FileSystem hdfs = null;
        Path file = null;
        OutputStream os = null;
        BufferedWriter wd = null;
        try
        {
            hdfs = FileSystem.get(this.conf);
            file = new Path(
                    Properties.getString("rts.storm.hdfs_output_file") + this.id);
            if (hdfs.exists(file))
                os = hdfs.append(file);
            else
                os = hdfs.create(file);
            wd = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            for (String tweet_score : tweet_scores)
            {
                wd.write(tweet_score);
            }
        }
        catch (IOException ex)
        {
            LOGGER.error("Failed to write tweet score to HDFS", ex);
            LOGGER.trace(null, ex);
        }
        finally
        {
            try
            {
                if (wd != null) wd.close();
                if (os != null) os.close();
                if (hdfs != null) hdfs.close();
            }
            catch (IOException ex)
            {
                LOGGER.fatal("IO Exception thrown while closing HDFS", ex);
                LOGGER.trace(null, ex);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) { }

    private Configuration getHdfsConfiguration(){
        Configuration conf = new Configuration();
        conf.addResource(new Path(Properties.getString("hadoop.core.site.xml.location")));//"/opt/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path(Properties.getString("hadoop.hdfs.site.xml.location")));//"/opt/hadoop/etc/hadoop/hdfs-site.xml"));
        return conf;
    }
}
