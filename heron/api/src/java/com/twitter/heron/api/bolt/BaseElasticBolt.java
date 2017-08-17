//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.api.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.twitter.heron.api.topology.BaseComponent;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;

/**
 * Created by zhengyang on 25/6/17.
 */
public abstract class BaseElasticBolt extends BaseComponent implements IElasticBolt {
  private static final long serialVersionUID = 4309732999277305080L;
  private int numCore = -1;
  private boolean initialized = false;
  private ArrayList<LinkedList<Tuple>> queueArray;
  private ConcurrentHashMap<String, Integer> stateMap;
  private ArrayList<BaseElasthread> threadArray;
  private ConcurrentLinkedQueue<BaseCollectorTuple> collectorQueue;
  private OutputCollector collector;

  public void test() {
    System.out.println("Num Cores: " + numCore);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public void execute(Tuple tuple) {
  }

  public void initElasticBolt(OutputCollector acollector){
    if (!initialized){
      initialized = true;
    }
//    numCore = 3 ; // temporarily set to 2 for testing purpose
    queueArray = new ArrayList<>();
    stateMap = new ConcurrentHashMap<>();
    for (int i = 0; i < numCore; i++) {
      queueArray.add(new LinkedList<>());
    }
    threadArray = new ArrayList<>();

    for (int i = 0; i < numCore; i++){
      threadArray.add(new BaseElasthread( String.valueOf(i), this));
    }
    collector = acollector;
    collectorQueue = new ConcurrentLinkedQueue<>();
  }

  public LinkedList<Tuple> getQueue(int i){
    return queueArray.get(i);
  }

  public final void runBolt(){
    System.out.println("RUNNINGBOLT!!!");
    try {
      for (int i = 0; i < numCore; i++) {
        if (!getQueue(i).isEmpty()) {
          if (threadArray.get(i) == null) {
            threadArray.add(new BaseElasthread(String.valueOf(i), this));
          }
          threadArray.get(i).start();
        }
      }
      printStateMap();
      while (!collectorQueue.isEmpty()){
        BaseCollectorTuple next = collectorQueue.poll();
        collector.emit(next.getT(), new Values(next.getS()));
      }
    } catch (Exception e){
      System.out.println("runBoltError");
      System.out.println(e);
    }

    Utils.sleep(5000);
  }

  public int getNumCore() {
    return numCore;
  }

  public void setNumCore(int numCore){
    this.numCore = numCore;
  }

  public void loadTuples(Tuple t){
    try {
      queueArray.get(Math.abs(t.hashCode())%this.numCore).add(t);
    } catch (Exception e) {
      System.out.println("loadingError");
      System.out.println(e);
    }
  }

  public void loadOutputTuples(Tuple t, String s){
    BaseCollectorTuple output = new BaseCollectorTuple(t,s);
    collectorQueue.add(output);
  }


  public synchronized void updateState(String tuple, Integer number) {
    try {
      if (stateMap.get(tuple) == null) {
        stateMap.put(tuple, number);
      } else {
        int amount = stateMap.get(tuple);
        stateMap.put(tuple, amount + number);
      }
    } catch (Exception e){
      System.out.println("updateStateError");
      System.out.println(e);
    }
  }

  public void printStateMap(){
    System.out.println(Collections.singletonList(stateMap));
  }
}