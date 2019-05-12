package io.bittiger.crawler;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class Main {
    private static String QUEUE_NAME = "q_demo";


    private static void topic_exchange() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("e_topic_logs", "topic",true);

        String message = "topic error  message cs502-1802 demo msg: check null pointer now";
        //wild card
        String routingKey = "10.warn.level1";
        String routingKey2 = "12.warn";


        channel.basicPublish("e_topic_logs", routingKey, null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
        System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");

        channel.close();
        connection.close();

    }

    private static void fanout_exchange() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("e_fanout_demo", "fanout",true);

        String message = "broadcast message hello...";

        channel.basicPublish("e_fanout_demo", "", null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }

    private static void queue_withoutexchange() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //channel.exchangeDeclare();
        channel.queueDeclare("q_demo", true, false, false, null);
        String msg = "hello, CS502 student";
        System.out.println(" [x] Sent '" + msg + "'");

        channel.basicPublish("", "q_demo", null, msg.getBytes("UTF-8"));

        Channel channel2 = connection.createChannel();
        String msg2 = "hello, rabbit2";
        System.out.println(" [x] Sent '" + msg2 + "'");
        channel2.exchangeDeclare("ex_test" , "direct", true );

        channel2.queueDeclare("q_test", true, false, false, null);
        channel2.basicPublish("ex_test", "", null, msg2.getBytes("UTF-8"));

        channel2.close();
        channel.close();
        connection.close();

    }

    private static void publishCrawlerFeed() throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare("q_feeds", true, false, false, null);
        //class Feed {String category; String url;}
        String msg = "Philips Waterproof Shaver, 6.0, 8153,20";
        System.out.println(" [x] Sent '" + msg + "'");

        channel.basicPublish("", "q_feeds", null, msg.getBytes("UTF-8"));
        String msg2 = "mountain bike, 13.8, 8143, 19";
        System.out.println(" [x] Sent '" + msg2 + "'");

        channel.basicPublish("", "q_feeds", null, msg2.getBytes("UTF-8"));
        channel.close();
        connection.close();
    }

    private static void publish_direct_exchange() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare("ex_demo_direct","direct", true);
        String msg = "image from nyc";
        channel.basicPublish("ex_demo_direct", "image", null, msg.getBytes("UTF-8"));
        System.out.println(" [x] Sent '" + msg + "'");
        channel.close();
        connection.close();
    }


    public static void main(String[] args) throws Exception{
        //publish_direct_exchange();
        //queue_withoutexchange();

        //fanout_exchange();
        //topic_exchange();
        publishCrawlerFeed();
    }
}
