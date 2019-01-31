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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.oneops.cms.cm.ops.domain.OpsActionState;
import com.oneops.cms.simple.domain.CmsActionOrderSimple;
import com.oneops.cms.simple.domain.CmsCISimple;
import com.oneops.cms.simple.domain.CmsRfcCISimple;
import com.oneops.cms.simple.domain.CmsWorkOrderSimple;
import com.oneops.cms.util.CmsUtil;
import java.io.Console;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class WoListener implements MessageListener {

  private final int listenerId;
  private final Gson gson = new Gson();
  private final Session session;
  private final MessageProducer producer;
  private final String[] stateCycle;
  private final long sleepTime;

  private int stateCycleCounter = 0;

  public WoListener(int id, Session session, MessageProducer producer, String stateCycle, long sleepTime) {
    this.listenerId = id;
    this.session = session;
    this.producer = producer;
    this.stateCycle = stateCycle.split(",");
    this.sleepTime = sleepTime;
  }

  @Override
  public void onMessage(Message msg) {
    try {
      if (msg instanceof TextMessage) {
        String corelationId = msg.getJMSCorrelationID();
        String type = msg.getStringProperty("type");
        boolean isBatch = msg.getBooleanProperty("batch");

        if (type.equals(CmsUtil.ACTION_ORDER_TYPE)) {
          CmsActionOrderSimple ao = gson.fromJson(((TextMessage) msg).getText(), CmsActionOrderSimple.class);
          log("received action order " + ao.getActionId());
          Thread.sleep(this.sleepTime);
          processActionOrder(ao, corelationId, stateCycle[stateCycleCounter % stateCycle.length]);
          log("processed action order " + ao.getActionId() + " - " + ao.getActionState().getName().toUpperCase());
          stateCycleCounter++;
        } else {
          List<CmsWorkOrderSimple> wos;
          if (isBatch) {
            wos = gson.fromJson(((TextMessage) msg).getText(), (new TypeToken<ArrayList<CmsWorkOrderSimple>>() {
            }).getType());
            log("received work order batch of " + wos.size());
          } else {
            wos = new ArrayList<>(1);
            CmsWorkOrderSimple wo = gson.fromJson(((TextMessage) msg).getText(), CmsWorkOrderSimple.class);
            wos.add(wo);
            log("received work order " + wo.rfcCi.getRfcId());
          }

          Console console = System.console();
          for (int i = 0, batchSize = wos.size(); i < batchSize; i++) {
            String state;
            state = stateCycle[stateCycleCounter % stateCycle.length];
            if (state.equals("?") || state.equals("prompt")) {
              state = console.readLine("Complete or Fail? ('c', 'f'): ").toLowerCase().startsWith("c") ? "complete" : "failed";
            }
            Thread.sleep(this.sleepTime);
            CmsWorkOrderSimple wo = wos.get(i);
            CmsRfcCISimple rfc = wo.rfcCi;
            processWorkOrder(wo, corelationId, state);
            log("processed work order " + rfc.getRfcId() + " - " + state.toUpperCase() + " (" + rfc.getCiClassName() + " " + rfc.getCiName() + ")");
            if (!"complete".equals(state) && i < batchSize - 1) {
              log("forcing rest of work order batch  - " + state.toUpperCase());
              for (int j = i + 1; j < batchSize; j ++) {
                processWorkOrder(wos.get(j), corelationId, state);
              }
              i = batchSize;  // to break out of the loop
            }
            stateCycleCounter++;
          }
        }
      }
    } catch (Throwable e) {
      log("Failed to process: " + msg);
      e.printStackTrace();
    }
  }

  private void processActionOrder(CmsActionOrderSimple ao, String corelationId, String state) throws JMSException {
    TextMessage responseMsg = session.createTextMessage();
    responseMsg.setJMSCorrelationID(corelationId);
    responseMsg.setStringProperty("type", CmsUtil.ACTION_ORDER_TYPE);

    if (state.equalsIgnoreCase("complete")) {
      ao.setActionState(OpsActionState.complete);
      CmsCISimple inputCi = ao.getCi();
      CmsCISimple ci = new CmsCISimple();
      ci.setCiId(inputCi.getCiId());
      ci.setCiClassName(inputCi.getCiClassName());
      ci.getAttrProps().putAll(inputCi.getAttrProps());
      for (Map.Entry<String, String> attr : inputCi.getCiAttributes().entrySet()) {
        ci.addCiAttribute(attr.getKey(), attr.getValue());
      }
      if (ci.getCiAttributes().containsKey("max_instances")) {
        ci.getCiAttributes().put("max_instances", "1500");
      }
      ao.setResultCi(ci);
      responseMsg.setStringProperty("task_result_code", "200");
    } else {
      ao.setActionState(OpsActionState.failed);
      responseMsg.setStringProperty("task_result_code", "404");
    }
    responseMsg.setStringProperty("type", "opsprocedure");
    responseMsg.setText(gson.toJson(ao));
    producer.send(responseMsg);
    session.commit();
  }


  private void processWorkOrder(CmsWorkOrderSimple wo, String corelationId, String status) throws JMSException {
    TextMessage responseMsg = session.createTextMessage();
    responseMsg.setJMSCorrelationID(corelationId);
    responseMsg.setStringProperty("type", CmsUtil.WORK_ORDER_TYPE);

    if (status.equalsIgnoreCase("complete")) {
      CmsRfcCISimple rfc = wo.getRfcCi();
      CmsCISimple ci = new CmsCISimple();
      ci.setCiId(rfc.getCiId());
      ci.setCiClassName(rfc.getCiClassName());
      ci.getAttrProps().putAll(rfc.getCiAttrProps());
      for (Map.Entry<String, String> attr : rfc.getCiAttributes().entrySet()) {
        ci.addCiAttribute(attr.getKey(), attr.getValue());
      }
      wo.setResultCi(ci);
      wo.setDpmtRecordState("complete");
      responseMsg.setStringProperty("task_result_code", "200");
    } else {
      wo.setDpmtRecordState("failed");
      responseMsg.setStringProperty("task_result_code", "404");
      wo.setComments("Failed by inductor stub");
    }
    responseMsg.setStringProperty("type", "deploybom");
    responseMsg.setText(gson.toJson(wo));
    producer.send(responseMsg);
    session.commit();
  }

  private void log(String message) {
    System.out.println("Listener " + listenerId + " " + message);
  }
}
