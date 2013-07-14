/*
 *RabbitMQmanager
 *Simple rabbitmq processes 
 * create/close channel
 * declere/delete queues
 * send/recieve obj
 * to install server : http://www.rabbitmq.com/
 * 
 * written for my intership at EBA(eba.gov.tr)
 */
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ahmedtalha
 */
 class RabbitMQManager {

    public static Connection connection1;
    public static Channel channel1;
    public DeclareOk result;
    public QueueingConsumer watcher1 = new QueueingConsumer(channel1);
    
    public RabbitMQManager(){
    
    }
    
    public void createChannel(String userName,String pass,String host) {
        try {
            ConnectionFactory fact = new ConnectionFactory();
            fact.setUsername(userName);
            fact.setPassword(pass);
            fact.setVirtualHost("/");		
            fact.setHost(host);
            fact.setPort(5672);
            connection1 = fact.newConnection();
            channel1 = connection1.createChannel();
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQManager.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public void closeChannel() {
        try {
            if(channel1 != null){
                channel1.close();
            }
            if(connection1 != null){
 
                connection1.close();

            }
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQManager.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void decleraQ(String qName) throws IOException {

        channel1.queueDeclare(qName, false, false, false,
                null);

    }

    public void deleteQ(String qName) throws IOException {

        channel1.queueDelete(qName);
    }

    public void sendMessageToQueue(Object obj,String queueName) {
        try {
            
            channel1.queueDeclare(queueName, false, false, false, null);

            channel1.basicPublish("", queueName,
                    MessageProperties.PERSISTENT_TEXT_PLAIN, returnBytes(obj));
            
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQManager.class.getName()).log(Level.SEVERE, null, ex);
        }

    

    }

    public byte[] returnBytes(Object obj) throws IOException {

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(os);
        out.writeObject(obj);

        byte byteForm[] = os.toByteArray();

        out.close();
        os.close();

        return byteForm;

    }

    public int countNum() {

        return result.getMessageCount();

    }

     public Object recieveMessageFromQueue(String qName) throws IOException, ShutdownSignalException,
      ConsumerCancelledException, InterruptedException,
      ClassNotFoundException {
      
      channel1.basicConsume(qName, false, watcher1);
      
      QueueingConsumer.Delivery delivery = watcher1.nextDelivery();
      
      Object obj2;
      
      obj2 = openBytes(delivery.getBody());
      
      
      return obj2;
      
      }
      
      public Object openBytes(byte[] body) throws IOException,
      ClassNotFoundException {
      
      ByteArrayInputStream is = new ByteArrayInputStream(body);
      ObjectInputStream in = new ObjectInputStream(is);
      
      Object obj = null;
      obj = in.readObject();
      
      return obj;
      }
}
