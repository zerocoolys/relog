package com.ss.main;

import com.ss.mq.producer.LogProducer;
import com.ss.quartz.TimerManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.time.ZoneId;
import java.util.TimeZone;

/**
 * Created by dolphineor on 2015-5-27.
 */
public class RelogProducerMain {

    public static void main(String[] args) {
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Shanghai")));
        TimerManager.startJob();

        // initialize config
        RelogConfig.setMode(args[0]);
        RelogConfig.setTopic(args[1]);
        int port = Integer.parseInt(args[2]);

        ServerBootstrap bootstrap = new ServerBootstrap();

        NioEventLoopGroup bossGroup = new NioEventLoopGroup(2);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        final LogProducer producer = new LogProducer(RelogConfig.getTopic());

        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new EsChannelInitializer(producer))
                .option(ChannelOption.SO_BACKLOG, 1024);

        try {
            Channel channel = bootstrap.bind(port).sync().channel();
            System.out.println("Producer start finished...");

            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                producer.close();
            }
        });

    }
}
