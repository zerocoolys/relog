package com.zyx.main;

import com.zyx.mq.producer.LogProducer;
import com.zyx.quartz.TimerManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.internal.PlatformDependent;

import java.time.ZoneId;
import java.util.TimeZone;

/**
 * Created by dolphineor on 2015-5-27.
 */
public class RelogProducerMain {

    private static final int WORKER_THREAD_NUM = Runtime.getRuntime().availableProcessors() * 2;
    private static final int TCP_BACKLOG = 2048;

    public static void main(String[] args) {
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Shanghai")));
        TimerManager.startJob();

        // initialize config
        RelogConfig.setMode(args[0]);
        RelogConfig.setTopic(args[1]);
        int port = Integer.parseInt(args[2]);


        EventLoopGroup bossGroup = null;
        EventLoopGroup workerGroup = null;

        Class<? extends ServerSocketChannel> clz = null;


        if (Epoll.isAvailable()) {
            doRun(new EpollEventLoopGroup(), new EpollEventLoopGroup(WORKER_THREAD_NUM), EpollServerSocketChannel.class, port);
        } else {

            doRun(new NioEventLoopGroup(), new NioEventLoopGroup(WORKER_THREAD_NUM), NioServerSocketChannel.class, port);
        }
    }

    private static void doRun(EventLoopGroup bossGroup, EventLoopGroup workerGroup, Class<? extends ServerSocketChannel> clz, int port) {

        ServerBootstrap bootstrap = new ServerBootstrap();

        final LogProducer producer = new LogProducer(RelogConfig.getTopic());

        bootstrap.group(bossGroup, workerGroup)
                .channel(clz)
                .childHandler(new EsChannelInitializer(producer))
                .option(ChannelOption.SO_BACKLOG, TCP_BACKLOG)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.MAX_MESSAGES_PER_READ, Integer.MAX_VALUE)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(PlatformDependent.directBufferPreferred()));

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
