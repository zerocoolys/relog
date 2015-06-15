package com.ss.main;

import com.ss.quartz.TimerManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Created by dolphineor on 2015-5-27.
 */
public class RelogProducerMain {

    public static void main(String[] args) {
        TimerManager.startJob();

        // initialize config
        RelogConfig.setMode(args[0]);
        RelogConfig.setTopic(args[1]);
        int port = Integer.parseInt(args[2]);

        ServerBootstrap bootstrap = new ServerBootstrap();

        NioEventLoopGroup bossGroup = new NioEventLoopGroup(2);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new EsChannelInitializer())
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
    }
}
