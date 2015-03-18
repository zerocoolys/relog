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
public class HttpDecoderHandler extends ChannelInboundHandlerAdapter implements Constants {

    private static final String TRACKID = "t";


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;

            // req.getUri().contains("?t="); 判断是否包含track id
            if (req.getDecoderResult().isSuccess() && req.getUri().contains("?t=")) {
                QueryStringDecoder decoder = new QueryStringDecoder(req.getUri());

                if (decoder.parameters().get(TRACKID) != null) {
                    Jedis jedis = null;
                    try {
                        jedis = JRedisPools.getConnection();

                        MessageObject mo = new MessageObject();
                        mo.setHttpMessage(req);
                        String remote = ctx.channel().remoteAddress().toString().substring(1);
                        mo.add(REMOTE, remote);
                        mo.add(METHOD, req.getMethod().toString());
                        mo.add(VERSION, req.getProtocolVersion().toString());
                        mo.add(UNIX_TIME, Instant.now().getEpochSecond());

                        String remoteIp = remote.split(":")[0];
                        String city = jedis.hget(IP_AREA_INFO, remoteIp);
                        if (city == null) {
                            city = IPParser.getArea(remoteIp);
                            jedis.hset(IP_AREA_INFO, remoteIp, city);
                        }
                        mo.add(CITY, city);

                        Map<String, Object> source = new HashMap<>();
                        source.putAll(mo.getAttribute());
                        mo.getHttpMessage().headers().entries().forEach(entry -> source.put(entry.getKey(), entry.getValue()));

                        decoder.parameters().forEach((k, v) -> {
                            if (v.isEmpty())
                                source.put(k, "");
                            else
                                source.put(k, v.get(0));
                        });

                        jedis.lpush(ACCESS_MESSAGE, JSON.toJSONString(source));
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
