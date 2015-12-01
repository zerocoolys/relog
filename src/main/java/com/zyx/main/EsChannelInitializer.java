package com.zyx.main;

import com.zyx.mq.producer.LogProducer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

/**
 * Created by yousheng on 15/3/16.
 */
public class EsChannelInitializer extends ChannelInitializer {

    private final LogProducer producer;

    public EsChannelInitializer(LogProducer producer) {
        this.producer = producer;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
//        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new HttpRequestDecoder());
        pipeline.addLast(new HttpResponseEncoder());
        pipeline.addLast(new HttpContentCompressor());
        pipeline.addLast(new HttpDecoderHandler(producer));
    }
}
