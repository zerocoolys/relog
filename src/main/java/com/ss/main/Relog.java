package com.ss.main;

import com.ss.es.EsForward;
import com.ss.es.EsPools;
import com.ss.quartz.QuartzManager;
import com.ss.redis.RedisWorker;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yousheng on 15/3/16.
 */
public class Relog {

    public static void main(String[] args) {

        QuartzManager.startJob();

//        EsPools.setHost("182.92.227.79:19300");
//        EsPools.setClusterName("es-cluster,elasticsearch");
//        EsPools.setBulkRequestNumber(1000);
        // initialize elasticsearch
        EsPools.setHost(args[0]);
        EsPools.setClusterName(args[1]);
        EsPools.setBulkRequestNumber(Integer.parseInt(args[2]));

        List<EsForward> esForwards = new ArrayList<>();
        EsPools.getEsClient().forEach(client -> {
            esForwards.add(new EsForward(client));
        });

        new RedisWorker(esForwards);

        ServerBootstrap bootstrap = new ServerBootstrap();

        NioEventLoopGroup bossGroup = new NioEventLoopGroup(2);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new EsChannelInitializer())
                .option(ChannelOption.SO_BACKLOG, 1024);

        try {
            Channel channel = bootstrap.bind(28888).sync().channel();
            System.out.println("finished.");

            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }


    }
}
