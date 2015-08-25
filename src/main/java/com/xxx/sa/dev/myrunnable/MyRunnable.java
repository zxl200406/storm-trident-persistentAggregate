package com.xxx.sa.dev.myrunnable;


import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.xxx.sa.dev.jsonrpc.RpcClient;


public class MyRunnable implements Runnable
{

	public MyRunnable(BlockingQueue<String> queue){
		this.queue=queue;
	}

	public void run()
	{
		System.out.println("线程开始id="+Thread.currentThread().getId());
		int size=queue.size();
		final int  ConnectNumber=3;
		final RpcClient rcs = new RpcClient("beta.uaq.pt.xxx.com", 8433);

		boolean IsConnect=false;
		for(int i=0;i<ConnectNumber;i++){
			try{
				IsConnect=rcs.connect();
			}catch(Exception e){
				System.out.println(ExceptionUtils.getFullStackTrace(e));
			}
			if(IsConnect){
				break;
			}
		}



		while(size>0){
			String text=queue.poll();
			if(text==null){
				break;
			}

			if(IsConnect){
				if (!rcs.sendToSvrSync("Transfer.Update", text, 100)) {
					// logger.error("send item failed: " + list);
					System.out.println("send item failed: ");
				}	
				try {
					Thread.sleep(80);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}		

		}



		if(IsConnect){
			try{
				rcs.stopRpcCli();
			}catch(Exception e){
				System.out.println(ExceptionUtils.getFullStackTrace(e));
			}
		}
		System.out.println("线程结束id="+Thread.currentThread().getId());
		System.gc();

	}
	private BlockingQueue<String> queue;




}