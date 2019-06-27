//package com.examples;
//
//import kafka.javaapi.producer.Producer;
//import kafka.producer.KeyedMessage;
//import kafka.producer.Partitioner;
//import kafka.producer.ProducerConfig;
//import kafka.serializer.StringEncoder;
//import org.apache.commons.lang3.StringUtils;
//import scala.actors.threadpool.TimeUnit;
//
//import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.InputStreamReader;
//import java.util.Properties;
//
///**
// * Kafka生产者
// *
// * @author lisg
// */
//public class KafkaProducer {
//
//    public static void main(String[] args) throws Exception {
//        Properties props = new Properties();
//        //根据这个配置获取metadata,不必是kafka集群上的所有broker,但最好至少有两个
//        props.put("metadata.broker.list", "192.168.18.60:9092,192.168.18.61:9092");
//        //消息传递到broker时的序列化方式
//        props.put("serializer.class", StringEncoder.class.getName());
//        //zk集群
//        props.put("zookeeper.connect", "192.168.18.60:2181/kafka");
//        //是否获取反馈
//        //0是不获取反馈(消息有可能传输失败)
//        //1是获取消息传递给leader后反馈(其他副本有可能接受消息失败)
//        //-1是所有in-sync replicas接受到消息时的反馈
//        props.put("request.required.acks", "1");
////      props.put("partitioner.class", MyPartition.class.getName());
//
//        //创建Kafka的生产者, key是消息的key的类型, value是消息的类型
//        Producer<Integer, String> producer = new Producer<Integer, String>(
//                new ProducerConfig(props));
//
//        String topic = "user_report3";
//
//        int len = 0;
//        byte[] buf = new byte[1024];
//        BufferedReader bufferedReader = null;
//        int i = 10;
//        while (true) {
//
//            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/data2/hubei/20180907.log"), "UTF-8"));
//            String mess = "";
//            while ((mess = bufferedReader.readLine()) != null) {
//
////                System.out.println(msg);
//                if (!StringUtils.isEmpty(mess)) {
//                    KeyedMessage<Integer, String> keyedMessage = new KeyedMessage<Integer, String>(topic, mess);
//                    System.out.println(mess);
//                    producer.send(keyedMessage);
//                    TimeUnit.MILLISECONDS.sleep(100);
//                }
//
//            }
//        }
//
////      producer.close();
//    }
//
//}
//
///**
// * 自定义分区类
// */
//class MyPartition implements Partitioner {
//
//    public int partition(Object key, int numPartitions) {
//        return key.hashCode() % numPartitions;
//    }
//}
