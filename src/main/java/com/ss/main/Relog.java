package com.ss.main;

import com.ss.es.EsForward;
import com.ss.es.EsOperator;
import com.ss.es.EsPools;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Created by yousheng on 15/3/16.
 */
public class Relog {

    public static void main(String[] args) {
        // initialize elasticsearch
        EsPools.setHost(args[0]);
        EsPools.setPort(Integer.parseInt(args[1]));
        EsPools.setClusterName(args[2]);
        EsPools.setBulkRequestNumber(Integer.parseInt(args[3]));

        new EsForward();
        new EsOperator();

        ServerBootstrap bootstrap = new ServerBootstrap();

        NioEventLoopGroup bossGroup = new NioEventLoopGroup(2);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new EsChannelInitializer())
                .option(ChannelOption.SO_BACKLOG, 1024);

        try {
            Channel channel = bootstrap.bind(28888).sync().channel();
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }


    }
}
