package com.xxx.sa.dev.jsonrpc;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

import com.alibaba.fastjson.JSON;




public class RpcClient {



    private RpcClientConn rpcClientConn;

    private Channel       channel;

    private String        rpcSvrHostname;
    private Integer       rpcSvrPort;

    private Boolean       canRetry = false;

    private AtomicLong    counter  = new AtomicLong(0);

    /**
     * Start the Json Rpc Client <br/>
     * called once when new 'RpcClient' instance
     */
    public RpcClient(String hostname, Integer port){
        this.rpcSvrHostname = hostname;
        this.rpcSvrPort = port;
        this.rpcClientConn = new RpcClientConn(this, this.rpcSvrHostname, this.rpcSvrPort);
    }

    /**
     * Stop the Json Rpc Client
     */
    public void stopRpcCli() {
        canRetry = false;
        rpcClientConn.stop();
    }

    /**
     * Connect to Server<br/>
     * called after 'new RpcClient()' or 'disConnect()'
     * 
     * @return
     */
    public Boolean connect() {
        Boolean ret = rpcClientConn.startConnection();
        canRetry = true;
        return ret;
    }

    /**
     * Disconnect from Server
     */
    public void disConnect() {
        canRetry = false;
        rpcClientConn.stopConnection();
    }

    /**
     * Get Status of Client<br/>
     * use for debug only
     * 
     * @return "Open"|"Close"
     */
    public String status() {
        if (this.channel != null && this.channel.isOpen()) {
            return "Open";
        }

        return "Close";
    }

    /**
     * Send data to JsonRpc Server, Thread Safe
     * 
     * @param method, method of golang json rpc server. eg: Transfer.Update
     * @param params, object list
     * @param timeoutInMs, timeout in ms
     * @return true if sending ok, other false
     */
    public Boolean sendToSvrSync(String method, String params, Integer timeoutInMs) {
        ChannelFuture cf = sendToSvrAsync(method, params);
        if (cf == null) {
            return false;
        }

        try {
            return cf.awaitUninterruptibly(timeoutInMs);// ms
        } catch (Exception e) {
            return false;
        }
    }

    public ChannelFuture sendToSvrAsync(String method, String params) {
    	//JSON.parseArray(text)
    	JsonRpcObject jro = new JsonRpcObject(method, JSON.parseArray(params), counter.addAndGet(1));
    	String jsonStr=JSON.toJSONString(jro);
    	//System.out.println(jsonStr);
        if (this.channel != null && this.channel.isWritable()) {
            try {
                return this.channel.write(jsonStr);
            } catch (Exception e) {
                return null;
            }
        }

        return null;
    }

    // internal, retry after connection closed
    public void retryConnect() {
        if (!canRetry) {
            return;
        }

        Timer timer = new HashedWheelTimer();
        timer.newTimeout(new TimerTask() {

            public void run(Timeout timeout) throws Exception {
                rpcClientConn.startConnection();
            }
        }, 5, TimeUnit.SECONDS);
    }

    /**
     * Setters and Getters
     * 
     * @return
     */
    public RpcClientConn getRpcClientConn() {
        return rpcClientConn;
    }

    public void setRpcClientConn(RpcClientConn rpcCliConn) {
        this.rpcClientConn = rpcCliConn;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

}