package com.zyx.storm.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.zyx.storm.elasticsearch.common.DefaultEsTupleMapper;
import com.zyx.storm.elasticsearch.common.EsConfig;
import com.zyx.storm.elasticsearch.trident.EsStateFactory;
import com.zyx.storm.elasticsearch.trident.EsUpdater;
import com.zyx.storm.topologies.function.PrepareForES;
import com.zyx.storm.topologies.trident.FilterValidate;
import com.zyx.storm.topologies.trident.UpdateField;
import com.zyx.storm.topologies.trident.ValidityFilter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;

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

//        int numWorker = Integer.parseInt(cmd.getOptionValue("n"));
 //       String zkHost = cmd.getOptionValue("z");
//        String topic = cmd.getOptionValue("t");
        
        String zkHost = "192.168.1.110:2181";
        String topic = "domain10";

//        String id = cmd.getOptionValue("s");
//        int p = Integer.parseInt(cmd.getOptionValue("p"));
//
//        String zkRoot = "/brokers";


        BrokerHosts hosts = new ZkHosts(zkHost, "/brokers");

        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(hosts, topic);

        tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(tridentKafkaConfig);

        TridentTopology topology = new TridentTopology();

      //  EsConfig esConfig = new EsConfig("es-cluster", new String[]{"192.168.1.10:9200"});
        EsConfig esConfig = new EsConfig("es-intern-cluster", new String[]{"192.168.100.10:29300"});

        EsStateFactory esStateFactory = new EsStateFactory(esConfig, new DefaultEsTupleMapper());

      /*  topology.newStream("kafkaStream", spout)
                .each(new Fields("str"), new FilterValidate())
                .each(new Fields("str"), new UpdateField(), new Fields("data"))
                .persistentAggregate(esStateFactory,);*/
        
        topology.newStream("kafkaStream", spout)
        .each(new Fields("str"), new ValidityFilter())
   //     .each(new Fields("str"), new ToUpper(), new Fields("upperString"))
        .each(new Fields("str"), new PrepareForES(), new Fields("index", "type", "id", "source"))
        .partitionPersist(esStateFactory, new Fields("index", "type", "id", "source"), new EsUpdater(), new Fields());

//                .persistentAggregate(esStateFactory,new Field);

//        builder.setSpout("kafkaTridentTopo", "kafkaStrem", "1", kafkaSpout, 2, "batchGroup");
//
//        builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig), p);
//        builder.setBolt("commonsBolt", new CommonsBolt(), p);

        Config conf = new Config();
        conf.setDebug(true);

//        if (args != null && args.length > 0) {
//        conf.setNumWorkers(numWorker);
//        StormSubmitter.submitTopologyWithProgressBar("relogConsumerTopology", conf, topology.build());


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("relogConsumerTopology", conf, topology.build());
    }
}
