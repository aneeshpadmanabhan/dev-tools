/*******************************************************************************
 *
 * Copyright 2015 Walmart, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.oneops.inductor.stub;

import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class InductorStub {
  private static final String RESPONSE_QUEUE = "controller.response";

  public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      System.out.println("Usage: java -jar inductor-stub-fat.jar queue-name wo-result(complete/failed) sleep-time-in-ms listener-count");
      System.out.println("  Use '-D options to override default url ('amq.url', defaults to 'failover:(tcp://localhost:61616?keepAlive=true)?initialReconnectDelay=1000'), username ('amq.user', defaults to 'superuser') and password ('amq.pass', defaults to 'ilijailibu').");
      System.out.println("example:\n\t java -jar inductor-stub-fat.jar public.oneops.clouds.openstack.ind-wo complete 60000 10");
      return;
    }

    System.out.println("Creating listeners");
    new InductorStub().run(args[0], args[1], Long.parseLong(args[2]), Integer.parseInt(args[3]));
  }

  public void run(String requestQueueName, String status, long sleepTime, int threads) throws Exception {
    System.out.println("Listening on " + requestQueueName);

    String url = System.getenv("AMQ_URL");
    if (url == null) {
      url = System.getProperty("amq.url", "failover:(tcp://localhost:61616?keepAlive=true)?initialReconnectDelay=1000");
    }
    String user = System.getenv("AMQ_USER");
    if (user == null) {
      user = System.getProperty("amq.user", "superuser");
    }
    String pass = System.getenv("AMQ_PASS");
    if (pass == null) {
      pass = System.getProperty("amq.pass", "ilijailibu");
    }

    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
    factory.setUserName(user);
    factory.setPassword(pass);

    ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
    for (int i = 1; i <= threads; i++) {
      Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
      Queue requests = session.createQueue(requestQueueName);
      Queue responses = session.createQueue(RESPONSE_QUEUE);
      MessageConsumer consumer = session.createConsumer(requests);
      consumer.setMessageListener(new WoListener(i, session, session.createProducer(responses), status, sleepTime));
      System.out.println("Listener " + i + " is waiting for messages");
    }
    connection.start();
  }
}
