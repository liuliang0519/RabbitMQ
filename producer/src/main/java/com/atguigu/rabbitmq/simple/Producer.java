package com.atguigu.rabbitmq.simple;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Producer {
    public static void main(String[] args) throws IOException, TimeoutException {

        //创建连接
        Connection connection = Producer.getConnection();

        //创建信道
        Channel channel = connection.createChannel();

        /**
         * queue      参数1：队列名称
         * durable    参数2：是否定义持久化队列,当mq重启之后,还在
         * exclusive  参数3：是否独占本次连接
         *            ① 是否独占,只能有一个消费者监听这个队列
         *            ② 当connection关闭时,是否删除队列
         * autoDelete 参数4：是否在不使用的时候自动删除队列,当没有consumer时,自动删除
         * arguments  参数5：队列其它参数
         */
        channel.queueDeclare("simple_queue",true,false,false,null);

        //要发送的信息
        String msg = "丹丹是一直小兔几啊！！！!";

        /**
         *    把消息发送到交换机  由交换值转发到指定的 消息队列上
         *
         * 参数1：交换机名称,如果没有指定则使用默认Default Exchage
         * 参数2：路由key,简单模式可以传递队列名称
         * 参数3：配置信息
         * 参数4：消息内容
         */
        channel.basicPublish("","simple_queue",null,msg.getBytes());

        System.out.println("消息已发送！   => "+msg);


        //关闭资源
        channel.close();
        connection.close();


    }

    public static Connection getConnection(){
        //创建连接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        //设置主机地址
        connectionFactory.setHost("192.168.247.128");
        //java端默认连接端口
        connectionFactory.setPort(5672);
        //默认虚拟主机名称  /
        connectionFactory.setVirtualHost("/");
        //设置连接用户名 默认是guest
        connectionFactory.setUsername("admin");
        //设置连接密码 默认是guest
        connectionFactory.setPassword("123456");


        //创建连接
        Connection connection = null;
        try {
            connection = connectionFactory.newConnection();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        return connection;

    }
}
