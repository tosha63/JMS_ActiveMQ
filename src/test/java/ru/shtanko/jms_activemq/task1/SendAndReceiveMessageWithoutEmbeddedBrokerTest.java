package ru.shtanko.jms_activemq.task1;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.testng.annotations.*;

import javax.jms.*;
import java.net.URI;

import static org.testng.AssertJUnit.assertEquals;

public class SendAndReceiveMessageWithoutEmbeddedBrokerTest {
    private Session[] session;
    private Destination[] destination;
    private BrokerService broker;
    private Connection connection;


    @BeforeMethod
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);

        broker.start();
        URI uri = broker.getVmConnectorURI();
        ConnectionFactory factory = new ActiveMQConnectionFactory(uri);
        connection = factory.createConnection();
        connection.start();
        session = new Session[]{connection.createSession(false, Session.AUTO_ACKNOWLEDGE),
                connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE),
                connection.createSession(false, Session.CLIENT_ACKNOWLEDGE),
                /*connection.createSession(true, Session.SESSION_TRANSACTED)*/};

        destination = new Queue[session.length];
        for (int i = 0; i < session.length; i++) {
            destination[i] = session[i].createQueue("TestQueue" + i);
        }
    }

    @AfterMethod
    public void closingResources() {
        if (broker != null) {
            try {
                broker.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
        destination = null;
    }

    @Test
    public void sendAndReceiveMessagePersistent() throws Exception {
        String[] arrayExpected = new String[4];
        String[] arrayActual = new String[4];
        for (int i = 0; i < session.length; i++) {
            MessageProducer producer = session[i].createProducer(destination[i]);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage msg = session[i].createTextMessage("hello" + i);
            producer.send(msg);
            MessageConsumer consumer = session[i].createConsumer(destination[i]);
            TextMessage msgActual = (TextMessage) consumer.receive();
            arrayExpected[i] = "hello" + i;
            arrayActual[i] = msgActual.getText();

        }
        assertEquals(arrayExpected[0], arrayActual[0]);
        assertEquals(arrayExpected[1], arrayActual[1]);
        assertEquals(arrayExpected[2], arrayActual[2]);
//        assertEquals(arrayExpected[3], arrayActual[3]);
    }


    @Test
    public void sendMessageNonPersistent() throws Exception {
        String[] arrayExpected = new String[4];
        String[] arrayActual = new String[4];
        for (int i = 0; i < session.length; i++) {
            MessageProducer producer = session[i].createProducer(destination[i]);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            TextMessage msg = session[i].createTextMessage("hello" + i);
            producer.send(msg);
            MessageConsumer consumer = session[i].createConsumer(destination[i]);
            TextMessage msgActual = (TextMessage) consumer.receive();
            arrayExpected[i] = "hello" + i;
            arrayActual[i] = msgActual.getText();
        }
        assertEquals(arrayExpected[0], arrayActual[0]);
        assertEquals(arrayExpected[1], arrayActual[1]);
        assertEquals(arrayExpected[2], arrayActual[2]);
        //   assertEquals(arrayExpected[3], arrayActual[3]);
    }
}
