package com.ss.main;

import com.alibaba.fastjson.JSON;
import com.ss.config.JRedisPools;
import com.ss.vo.MessageObject;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import redis.clients.jedis.Jedis;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yousheng on 15/3/16.
 */
public class HttpDecoderHandler extends ChannelInboundHandlerAdapter {

    private static final String TRACKID = "t";


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;

            // req.getUri().contains("?t="); 判断是否包含track id
            if (req.getDecoderResult().isSuccess() && req.getUri().contains("?t=")) {
                QueryStringDecoder decoder = new QueryStringDecoder(req.getUri());

                if (decoder.parameters().get(TRACKID) != null) {
                    MessageObject mo = new MessageObject();
                    mo.setHttpMessage(req);
                    mo.add("remote", ctx.channel().remoteAddress().toString().substring(1));

                    Map<String, Object> source = new HashMap<>();

                    source.putAll(mo.getAttribute());
                    source.put("method", mo.getHttpMessage().getMethod());
                    source.put("version", mo.getHttpMessage().getProtocolVersion());
                    source.put("utime", Instant.now().getEpochSecond());
                    mo.getHttpMessage().headers().entries().forEach(entry -> source.put(entry.getKey(), entry.getValue()));

                    decoder.parameters().forEach((k, v) -> {
                        if (v.isEmpty())
                            source.put(k, "");
                        else
                            source.put(k, v.get(0));
                    });

                    //EsForward.push(mo);
                    Jedis jedis = null;
                    try {
                        jedis = JRedisPools.getConnection();
                        jedis.lpush(RedisForward.ACCESS_MESSAGE, JSON.toJSONString(source));
                    } finally {
                        JRedisPools.returnJedis(jedis);
                    }
                }

            }

            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

            ctx.write(response).addListener(ChannelFutureListener.CLOSE);

            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }
}
