package com.ss.main;

import com.alibaba.fastjson.JSON;
import com.ss.redis.JRedisPools;
import com.ss.vo.MessageObject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import redis.clients.jedis.Jedis;

import java.io.IOException;
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

//    private static final String TRACKID = "t";
//    private static final long COOKIE_EXPIRE = 31536000000l;

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

                if (decoder.parameters().get(T) != null) {
                    Map<String, Object> source = new HashMap<>();
                    Set<Cookie> cookies = null;
                    Jedis jedis = null;
                    try {
                        jedis = JRedisPools.getConnection();

                        MessageObject mo = new MessageObject();
                        mo.setHttpMessage(req);
                        String remote = ctx.channel().remoteAddress().toString().substring(1);
                        mo.add(REMOTE, remote);
                        mo.add(METHOD, req.getMethod().toString());
                        mo.add(VERSION, req.getProtocolVersion().toString());
                        mo.add(UNIX_TIME, System.currentTimeMillis());

                        String remoteIp = remote.split(":")[0];
                        String city = jedis.hget(IP_AREA_INFO, remoteIp);
                        if (city == null) {
                            city = IPParser.getArea(remoteIp);
                            jedis.hset(IP_AREA_INFO, remoteIp, city);
                        }
                        mo.add(CITY, city);

                        source.putAll(mo.getAttribute());
                        mo.getHttpMessage().headers().entries().forEach(entry -> source.put(entry.getKey(), entry.getValue()));

                        decoder.parameters().forEach((k, v) -> {
                            if (v.isEmpty())
                                source.put(k, "");
                            else
                                source.put(k, v.get(0));
                        });
                        source.remove("Referer");

                        cookies = handleCookies(req);
                        for (Cookie cookie : cookies) {
                            if (VID.equals(cookie.getName())) {
                                source.put(VID, cookie.getValue());
                                break;
                            }
                        }

                        jedis.lpush(ACCESS_MESSAGE, JSON.toJSONString(source));
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        JRedisPools.returnJedis(jedis);
                    }

                    writeResponse(ctx.channel(), cookies);
                }

            }

        }
    }

    private void writeResponse(Channel channel, Set<Cookie> cookies) {
        // Convert the response content to a ChannelBuffer.
//        byte[] bytes = Base64.getDecoder().decode(GifParser.base64String);
//        ByteBuf buf = copiedBuffer(bytes);
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

        cookies.forEach(cookie -> response.headers().add(SET_COOKIE, ServerCookieEncoder.encode(cookie)));

        // Write the response.
        ChannelFuture future = channel.writeAndFlush(response);
        // Close the connection after the write operation is done if necessary.
        if (close) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private Set<Cookie> handleCookies(HttpRequest request) {
        Set<Cookie> cookies;
        String value = request.headers().get(COOKIE);
        if (value == null) {
            cookies = Collections.emptySet();
//            cookies = new HashSet<>();
        } else {
            cookies = CookieDecoder.decode(value);
        }

//        boolean hasVid = false;
//        String vid;
//        if (!cookies.isEmpty()) {
//            for (Cookie cookie : cookies) {
//                if (VID.equals(cookie.getName())) {
//                    hasVid = true;
//                    break;
//                }
//            }
//
//            if (!hasVid) {
//                vid = UUID.randomUUID().toString().replaceAll("-", "");
//                Cookie _cookie = new DefaultCookie(VID, vid);
//                _cookie.setMaxAge(COOKIE_EXPIRE);
//                cookies.add(_cookie);
//            }
//        } else {
//            vid = UUID.randomUUID().toString().replaceAll("-", "");
//            Cookie _cookie = new DefaultCookie(VID, vid);
//            _cookie.setMaxAge(COOKIE_EXPIRE);
//            cookies.add(_cookie);
//        }

        return cookies;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.channel().close();
    }

}
