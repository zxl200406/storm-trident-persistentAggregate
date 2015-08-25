package com.xxx.sa.dev.jsonrpc;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

public class RpcClientMsgHandler extends SimpleChannelHandler {


    private RpcClient   rpcClient;

    public RpcClientMsgHandler(RpcClient rpcClient){
        super();
        this.rpcClient = rpcClient;
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        super.channelOpen(ctx, e);
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        rpcClient.setChannel(ctx.getChannel());
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        System.out.println("recv from svr:" + (String) e.getMessage());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    	System.out.println(e.getCause());
        e.getCause().printStackTrace();
        ctx.getChannel().close();
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
    	System.out.println("Disconnected from: " + ctx.getChannel().getRemoteAddress());
    }

    @Override
    public void channelClosed(final ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // 意外断开重连
    	System.out.println("channelClosed:" + ctx.getName());
        rpcClient.retryConnect();
    }
}