package com.examples;

/**
 * Created by ibf on 12/21.
 */
public class ConsumerTest {
    public static void main(String[] args) {
        String zookeeper = "192.168.18.60:2181,192.168.18.61:2181,192.168.18.63:2181/kafka";
        String groupId = "group2";
        String topic = "user_report";
        int threads = 1;

        JavaKafkaConsumerHighAPI example = new JavaKafkaConsumerHighAPI(topic, threads, zookeeper, groupId);
        new Thread(example).start();

  /*      // 执行10秒后结束
//        int sleepMillis = 1000000;
        try {
//            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 关闭*/
//        example.shutdown();
    }
}