package com.zyx.storm.topologies;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * Created by yousheng on 15/12/1.
 */
public class BaseTopology {

    private CommandLineParser parser;

    CommandLineParser getParser() {

        if (parser == null) {
            parser = new DefaultParser();
        }
        return parser;
    }


    Options getOptions() {
        Options options = new Options();

        options.addOption(new Option("t", true, "kafka topic name"))
                .addOption(new Option("z", true, "zookeeper connection string"))
                .addOption("n", true, "number of worker process")
                .addOption("p", true, "default parallelism_hint")
                .addOption("s", true, "spout id");

        return options;
    }


}
