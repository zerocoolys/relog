package com.ss.main;

import com.ss.vo.MessageObject;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

/**
 * Created by yousheng on 15/3/16.
 */
public class HttpDecoderHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;

            if (req.getDecoderResult().isSuccess()) {
                MessageObject mo = new MessageObject();
                mo.setHttpMessage(req);
                mo.add("remote", ctx.channel().remoteAddress().toString().substring(1));
                EsForward.push(mo);
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
