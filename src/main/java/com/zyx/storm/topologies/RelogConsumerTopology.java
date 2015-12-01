package com.zyx.storm.topologies;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.zyx.storm.bolts.CommonsBolt;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by yousheng on 15/12/1.
 */
public class RelogConsumerTopology extends BaseTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, NumberFormatException {

        RelogConsumerTopology instance = new RelogConsumerTopology();
        CommandLine cmd = null;
        try {
            cmd = instance.getParser().parse(instance.getOptions(), args);
        } catch (ParseException pe) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("topology", instance.getOptions());
            return;
        }

        int numWorker = Integer.parseInt(cmd.getOptionValue("n"));
        String zkHost = cmd.getOptionValue("z");
        String topic = cmd.getOptionValue("t");

        String id = cmd.getOptionValue("s");
        int p = Integer.parseInt(cmd.getOptionValue("p"));


        TopologyBuilder builder = new TopologyBuilder();


        BrokerHosts hosts = new ZkHosts(zkHost, "/brokers");

        String zkRoot = "/brokers";

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, id);


        builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig), p);
        builder.setBolt("commonsBolt", new CommonsBolt(), p);

        Config conf = new Config();
        conf.setDebug(true);

//        if (args != null && args.length > 0) {
        conf.setNumWorkers(numWorker);
        StormSubmitter.submitTopologyWithProgressBar("relogConsumerTopology", conf, builder.createTopology());
    }
}
