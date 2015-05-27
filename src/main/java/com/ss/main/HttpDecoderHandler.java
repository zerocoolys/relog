package com.ss.main;

import com.alibaba.fastjson.JSON;
import com.ss.mq.producer.LogProducer;
import com.ss.vo.MessageObject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

import java.util.*;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;

/**
 * Created by yousheng on 15/3/16.
 */
public class HttpDecoderHandler extends SimpleChannelInboundHandler<HttpObject> implements Constants {

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
                    Set<Cookie> cookies;

                    MessageObject mo = new MessageObject();
                    mo.setHttpMessage(req);
//                  String remote = ctx.channel().remoteAddress().toString().substring(1).split(":")[0];
//
//                  mo.method(req.getMethod().toString())
//                         .version(req.getProtocolVersion().toString())
//                         .remote(remote);

                    mo.add(REMOTE, req.headers().get(REAL_IP));
                    mo.add(METHOD, req.getMethod().toString());
                    mo.add(VERSION, req.getProtocolVersion().toString());
                    mo.add(UNIX_TIME, System.currentTimeMillis());

                    source.putAll(mo.getAttribute());
                    mo.getHttpMessage().headers().entries().forEach(entry -> source.put(entry.getKey(), entry.getValue()));

                    decoder.parameters().forEach((k, v) -> {
                        if (v.isEmpty())
                            source.put(k, "");
                        else
                            source.put(k, v.get(0));
                    });
                    source.remove("Referer");
                    if (source.containsKey(REAL_IP))
                        source.remove(REAL_IP);

                    cookies = handleCookies(req);
                    cookies.stream()
                            .filter(c -> VID.equals(c.getName()) || UCV.equals(c.getName()))
                            .forEach(cookie -> source.put(cookie.getName(), cookie.getValue()));
                    // send message
                    LogProducer producer = new LogProducer(RelogConfig.getKafkaTopic());
                    producer.handleMessage(JSON.toJSONString(source));
                    producer.close();

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
        if (value == null)
            cookies = new HashSet<>();
        else
            cookies = CookieDecoder.decode(value);


        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, -1);
        calendar.set(Calendar.MILLISECOND, 0);
        long expire = calendar.getTimeInMillis();

        boolean uvExists = false;
        String uvId;
        if (!cookies.isEmpty()) {
            for (Cookie cookie : cookies) {
                if (UCV.equals(cookie.getName())) {
                    uvExists = true;
                    break;
                }
            }

            if (!uvExists) {
                uvId = UUID.randomUUID().toString().replaceAll("-", "");
                Cookie _cookie = new DefaultCookie(UCV, uvId);
                _cookie.setMaxAge(expire);
                cookies.add(_cookie);
            }
        } else {
            uvId = UUID.randomUUID().toString().replaceAll("-", "");
            Cookie _cookie = new DefaultCookie(UCV, uvId);
            _cookie.setMaxAge(expire);
            cookies.add(_cookie);
        }

        return cookies;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.channel().close();
    }

}
