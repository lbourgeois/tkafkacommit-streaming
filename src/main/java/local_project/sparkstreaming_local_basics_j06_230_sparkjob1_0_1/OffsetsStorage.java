package local_project.sparkstreaming_local_basics_j06_230_sparkjob1_0_1;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.spark.streaming.kafka010.OffsetRange;
import scala.Tuple2;

public class OffsetsStorage {

    private final ZkUtils zkUtils;

    public OffsetsStorage(String zkUrl, int zkSessionTimeout, int zkConnectionTimeout) {
        Tuple2<ZkClient, ZkConnection> zkClientAndConnection = ZkUtils.createZkClientAndConnection("zookeeper0.weave.local", 10000, 10000);
        this.zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false);
    }

    public void storeProcessingOffets(String groupId, String kafka_topic, OffsetRange[] ranges) {
        this.zkUtils.updatePersistentPath("/consumers/" + groupId + "/offsets/" + kafka_topic + "/", ranges.toString(), null);
    }

}