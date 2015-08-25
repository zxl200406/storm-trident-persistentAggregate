package com.xxx.sa.dev.memdb;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;







import com.xxx.sa.dev.myrunnable.MyRunnable;

import storm.trident.state.ITupleCollection;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.snapshot.Snapshottable;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

public class MemoryDB<T> implements Snapshottable<T>, ITupleCollection, MapState<T> {
	MemoryMapStateBacking<OpaqueValue> _backing;
	SnapshottableMap<T> _delegate;



	public MemoryDB(String id) {
		_backing = new MemoryMapStateBacking(id);
		_delegate = new SnapshottableMap(OpaqueMap.build(_backing), new Values("$MEMORY-MAP-STATE-GLOBATOTALL$"));

	}

	public T update(ValueUpdater updater) {

		return _delegate.update(updater);
	}

	public void set(T o) {
		_delegate.set(o);
	}

	public T get() {
		return _delegate.get();
	}

	public void beginCommit(Long txid) {
		_delegate.beginCommit(txid);
	}

	public void commit(Long txid) {
		_delegate.commit(txid);
	}

	public Iterator<List<Object>> getTuples() {
		return _backing.getTuples();
	}

	public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
		return _delegate.multiUpdate(keys, updaters);
	}

	public void multiPut(List<List<Object>> keys, List<T> vals) {
		_delegate.multiPut(keys, vals);
	}

	public List<T> multiGet(List<List<Object>> keys) {
		return _delegate.multiGet(keys);
	}

	public static class Factory implements StateFactory {
		String _id;
		public Factory() {
			_id = UUID.randomUUID().toString();
		}
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new MemoryDB(_id + partitionIndex);
		}
	}

	static ConcurrentHashMap<String, Map<List<Object>, Object>> _dbs = new ConcurrentHashMap<String, Map<List<Object>, Object>>();

	static class MemoryMapStateBacking<T> implements IBackingMap<T>, ITupleCollection {

		private final int threadnum=10;
		private ExecutorService executor = Executors.newFixedThreadPool(threadnum);



		private void WriteFalcon(BlockingQueue<String> queue){
			System.out.println("准备写入");
			int size=queue.size();
			System.out.println("准备写入 queue.size()="+size);
			if(size<=0){
				return;
			}
			
			
			//大于10个包，用多线程发送数据
			if(size>10){
				//这里开启多个线程
				for(int j=0;j<threadnum;j++){
					MyRunnable worker=new MyRunnable(queue);
					executor.execute(worker);
				}
			}else{
				//这里只开启一个线程
				MyRunnable worker=new MyRunnable(queue);
				executor.execute(worker);
			}
		}



		public static void clearAll() {
			_dbs.clear();
		}
		Map<List<Object>, T> db;
		Long currTx;


		public MemoryMapStateBacking(String id) {
			if (!_dbs.containsKey(id)) {
				_dbs.put(id, new HashMap());
			}
			this.db = (Map<List<Object>, T>) _dbs.get(id);
		}

		public List<T> multiGet(List<List<Object>> keys) {
			List<T> ret = new ArrayList();
			for (List<Object> key : keys) {
				ret.add(db.get(key));
			}
			return ret;
		}

		public void multiPut(List<List<Object>> keys, List<T> vals) {
			for (int i = 0; i < keys.size(); i++) {
				List<Object> key = keys.get(i);
				T val = vals.get(i);     

				db.put(key, val);
				OpaqueValue o=(OpaqueValue) val;     
				
				
				
				DB currentdb= (DB)o.getCurr();   
				BlockingQueue<String> queue=currentdb.GetData();
				System.gc();
				if(queue==null){
					
					return;
				}			
				WriteFalcon(queue);			
				
			}
		}


		public Iterator<List<Object>> getTuples() {
			return new Iterator<List<Object>>() {

				private Iterator<Map.Entry<List<Object>, T>> it = db.entrySet().iterator();

				public boolean hasNext() {
					return it.hasNext();
				}

				public List<Object> next() {

					Map.Entry<List<Object>, T> e = it.next();
					List<Object> ret = new ArrayList<Object>();
					ret.addAll(e.getKey());
					ret.add(((OpaqueValue)e.getValue()).getCurr());
					return ret;
				}

				public void remove() {
					throw new UnsupportedOperationException("Not supported yet.");
				}
			};
		}
	}
}