package com.xxx.sa.dev.uqv;

import com.xxx.sa.dev.functions.AggregeteBatch;
import com.xxx.sa.dev.functions.Ip;
import com.xxx.sa.dev.functions.Location;
import com.xxx.sa.dev.functions.LocationIp;
import com.xxx.sa.dev.functions.LocationOp;
import com.xxx.sa.dev.functions.LocationOpIp;
import com.xxx.sa.dev.functions.Op;
import com.xxx.sa.dev.functions.OpIp;
import com.xxx.sa.dev.memdb.DB;
import com.xxx.sa.dev.memdb.MemoryDB;

import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;


public class TopologyApp 
{

	private String KafkaTopic;//uaq
	private String Jifang;//lg
	public String KafkaSpoutZkPath;//组合而成，在storm端的zk
	private final BrokerHosts brokerHosts;
	private String kafkaSpoutId = "KafkaSpoutIduaq";
	private int KafkaFetchBuffer=15;

	public TopologyApp(String KafkaZookeeperPath,String KafkaRelativePath,String KafkaTopic,String Jifang,String KafkaFetchBuffer){
		this.KafkaTopic=KafkaTopic;
		this.Jifang=Jifang;
		this.KafkaSpoutZkPath="/storm/kafka/"+KafkaRelativePath+"/"+KafkaTopic+"_"+Jifang+"_";
		brokerHosts = new ZkHosts(KafkaZookeeperPath, "/kafka/"+KafkaRelativePath+"/brokers");
		int num=Integer.parseInt(KafkaFetchBuffer);
		if(num>1){
			this.KafkaFetchBuffer=num;
		}

	}
	private StormTopology buildTopology() {
		// TODO Auto-generated method stub
		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts,KafkaTopic,"TridentKafkaConfig"+this.KafkaTopic);

		kafkaConfig.scheme = new SchemeAsMultiScheme(new KafkaScheme());
		kafkaConfig.forceFromStart = true;
		kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

		kafkaConfig.fetchSizeBytes = KafkaFetchBuffer << 20; // about 25M
		kafkaConfig.bufferSizeBytes = KafkaFetchBuffer << 20;

		TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);
		TridentTopology topology = new TridentTopology();


		storm.trident.Stream Origin_Stream=topology.newStream(kafkaSpoutId,kafkaSpout)
				.parallelismHint(64)
				.each(new Fields("msg"), new Splitfield(), 
						new Fields("time","srcip","dstip","src_port","dst_port","protocal","dataobj","src_country","src_province","src_city","src_isp"))
						.project(new Fields("time","srcip","dstip","src_port","dst_port","protocal","dataobj","src_country","src_province","src_city","src_isp"));

		/*,地区现在的定义，只是省
		 * 1.地区
		 * 2.地区+IP
		 * 3.地区+运营商
		 * 4.地区+运营商+IP
		 * ​5.IP
		 * 6.运营商
		 * 7.运营商+IP
		 */
		///////////////////////////////
		//功能一 地区   area
		Origin_Stream.partitionAggregate(new Fields("time","src_province","dataobj") , new Location(), 
				new Fields("LocationKey","LocationValue"))
				.aggregate(new Fields("LocationKey","LocationValue"),new AggregeteBatch(),new Fields("LocationKey_Agg","LocationValue_Agg"))
				.persistentAggregate(new MemoryDB.Factory(), new Fields("LocationKey_Agg","LocationValue_Agg"), new Counter(1), new Fields("func1"));

		//功能二 地区+IP   area=1,ip=10.202.10
		Origin_Stream.partitionAggregate(new Fields("time","src_province","dstip","dataobj") , new LocationIp(), 
				new Fields("LocationIpKey","LocationIpValue"))
				.aggregate(new Fields("LocationIpKey","LocationIpValue"),new AggregeteBatch(),new Fields("LocationIpKey_Agg","LocationIpValue_Agg"))
				.persistentAggregate(new MemoryDB.Factory(), new Fields("LocationIpKey_Agg","LocationIpValue_Agg"), new Counter(2), new Fields("func2"));

		//功能三 地区+运营商 area=1,op=2
		Origin_Stream.partitionAggregate(new Fields("time","src_province","src_isp","dataobj") , new LocationOp(), 
				new Fields("LocationOpKey","LocationOpValue"))
				.aggregate(new Fields("LocationOpKey","LocationOpValue"),new AggregeteBatch(),new Fields("LocationOpKey_Agg","LocationOpValue_Agg"))
				.persistentAggregate(new MemoryDB.Factory(), new Fields("LocationOpKey_Agg","LocationOpValue_Agg"), new Counter(3), new Fields("func3"));

		//功能四 地区+运营商+IP
		Origin_Stream.partitionAggregate(new Fields("time","src_province","src_isp","dstip","dataobj") , new LocationOpIp(), 
				new Fields("LocationOpIpKey","LocationOpIpValue"))
				.aggregate(new Fields("LocationOpIpKey","LocationOpIpValue"),new AggregeteBatch(),new Fields("LocationOpIpKey_Agg","LocationOpIpValue_Agg"))
				.persistentAggregate(new MemoryDB.Factory(), new Fields("LocationOpIpKey_Agg","LocationOpIpValue_Agg"), new Counter(4), new Fields("func4"));

		//功能五 IP
		Origin_Stream.partitionAggregate(new Fields("time","dstip","dataobj") , new Ip(), new Fields("IpKey","IpValue"))
		.aggregate(new Fields("IpKey","IpValue"),new AggregeteBatch(),new Fields("IpKey_Agg","IpValue_Agg"))
		.persistentAggregate(new MemoryDB.Factory(), new Fields("IpKey_Agg","IpValue_Agg"), new Counter(5), new Fields("func5"));

		//功能六 运营商
		Origin_Stream.partitionAggregate(new Fields("time","src_isp","dataobj") , new Op(), new Fields("OpKey","OpValue"))
		.aggregate(new Fields("OpKey","OpValue"),new AggregeteBatch(),new Fields("OpKey_Agg","OpValue_Agg"))
		.persistentAggregate(new MemoryDB.Factory(), new Fields("OpKey_Agg","OpValue_Agg"), new Counter(6), new Fields("func6"));

		//功能七 运营商+IP
		Origin_Stream.partitionAggregate(new Fields("time","src_isp","dstip","dataobj") , new OpIp(), new Fields("OpIpKey","OpIpValue"))
		.aggregate(new Fields("OpIpKey","OpIpValue"),new AggregeteBatch(),new Fields("OpIpKey_Agg","OpIpValue_Agg"))
		.persistentAggregate(new MemoryDB.Factory(), new Fields("OpIpKey_Agg","OpIpValue_Agg"), new Counter(7), new Fields("func7"));




		return topology.build();

	}
	public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException
	{
		if(args!=null&&args.length<4){
			if (args.length < 6) {
				System.err.println("Usage: ./bin/storm jar "
						+ "${your_topology-jar-with-dependencies.jar}" 
						+ "${Mainclass}" 
						+ "${topology_name} ${nimbus_host_ip} ${kafka's zookeeper list} ${KafkaRelativePath} ${Kafka_topic_name} ${Jifang} ${KafkaFetchBuffer}");
				System.exit(-1);  
			}
		}

		String TopologyName = args[0];
		String NimbusIp = args[1];     
		//创建topology
		TopologyApp MyTopology=new TopologyApp(args[2],args[3],args[4],args[5],args[6]);
		/**
		 * 创建配置
		 */
		Config config = new Config();
		config.put(Config.TOPOLOGY_ACKER_EXECUTORS, 100);
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 20);
		config.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT,MyTopology.KafkaSpoutZkPath);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);

		config.setNumWorkers(30);
		config.put(Config.NIMBUS_HOST, NimbusIp);     
		StormTopology stormTopology = MyTopology.buildTopology();
		StormSubmitter.submitTopology(TopologyName, config, stormTopology);

	}

}
