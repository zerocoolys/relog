package com.ss.main;

import com.alibaba.fastjson.JSON;
import com.ss.config.JRedisPools;
import com.ss.vo.MessageObject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;

/**
 * Created by yousheng on 15/3/16.
 */
public class HttpDecoderHandler extends SimpleChannelInboundHandler<HttpObject> implements Constants {

    private static final String TRACKID = "t";

    private final StringBuffer responseContent = new StringBuffer();

    private HttpRequest request;


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        messageReceived(ctx, msg);
    }

    private void messageReceived(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpRequest) {
            HttpRequest req = this.request = (HttpRequest) msg;

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
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        JRedisPools.returnJedis(jedis);
                    }

                    writeResponse(ctx.channel());
                }

            }

        }
    }

    private void writeResponse(Channel channel) {
        // Convert the response content to a ChannelBuffer.
        ByteBuf buf = copiedBuffer(responseContent.toString(), CharsetUtil.UTF_8);
        responseContent.setLength(0);

        // Decide whether to close the connection or not.
        boolean close = request.headers().contains(CONNECTION, HttpHeaders.Values.CLOSE, true)
                || request.getProtocolVersion().equals(HttpVersion.HTTP_1_0)
                && !request.headers().contains(CONNECTION, HttpHeaders.Values.KEEP_ALIVE, true);

        // Build the response object.
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buf);
//        response.headers().set(CONTENT_TYPE, "text/html; charset=UTF-8");
        response.headers().set(CONTENT_TYPE, "image/gif");

        if (!close) {
            // There's no need to add 'Content-Length' header
            // if this is the last response.
            response.headers().set(CONTENT_LENGTH, buf.readableBytes());
        }

        Set<Cookie> cookies;
        String value = request.headers().get(COOKIE);
        if (value == null) {
            cookies = Collections.emptySet();
        } else {
            cookies = CookieDecoder.decode(value);
        }
        if (!cookies.isEmpty()) {
            // Reset the cookies if necessary.
            for (Cookie cookie : cookies) {
                response.headers().add(SET_COOKIE, ServerCookieEncoder.encode(cookie));
            }
        }
        // Write the response.
        ChannelFuture future = channel.writeAndFlush(response);
        // Close the connection after the write operation is done if necessary.
        if (close) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.channel().close();
    }

}
