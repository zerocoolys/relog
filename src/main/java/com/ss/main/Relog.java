package com.ss.main;

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

        new EsForward();


        ServerBootstrap bootstrap = new ServerBootstrap();

        NioEventLoopGroup bossGroup = new NioEventLoopGroup(2);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        bootstrap.option(ChannelOption.SO_BACKLOG, 1024);
        bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(new EsChannelInitializer());

        try {
            Channel ch = bootstrap.bind(28888).sync().channel();

            ch.closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }


    }
}
