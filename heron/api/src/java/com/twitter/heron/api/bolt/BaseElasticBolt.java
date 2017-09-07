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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.twitter.heron.api.topology.BaseComponent;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

/**
 * Created by zhengyang on 25/6/17.
 * Assumptions made is that the key for the state are strings and the values and integers
 */
public abstract class BaseElasticBolt extends BaseComponent implements IElasticBolt {
  private static final long serialVersionUID = 4309732999277305080L;
  private int numCore = -1;
  // Serves as the map of queue of incoming work for the various threads
  private ArrayList<LinkedList<Tuple>> queueArray;
  // Serve as the inmemorystate of this instance of the ElasticBolt
  private ConcurrentHashMap<String, Integer> stateMap;
  // Keeps track of the threads in this ElasticBolt
  private ArrayList<BaseElasthread> threadArray;
  // As the collector is not threadsafe, this concurrent queue is
  // meant to join all the tuple outputs from various threads into a single threaded queue for the
  // queue to process
  private ConcurrentLinkedQueue<BaseCollectorTuple> collectorQueue;
  private OutputCollector collector;
  // this is used to synchronize/join all the threads in one iteration of processing
  private AtomicInteger lock;
  private long latency = System.currentTimeMillis();
  private HashMap<String, AtomicInteger> keyCountMap;
  private HashMap<String, Integer> keyThreadMap;
  private LinkedList<Integer> nextFreeQueue;

  @Override
  public void cleanup() {
  }

  @Override
  public void execute(Tuple tuple) {
  }

  /**
   * Initialize the Elasticbolt with the required data structures and threads
   * based on the topology
   *
   * The acollector is the OutputCollector used by this bolt to emit tuples
   * downstream
   *
   * @param acollector
   */
  public void initElasticBolt(OutputCollector acollector) {
    queueArray = new ArrayList<>();
    stateMap = new ConcurrentHashMap<>();
    for (int i = 0; i < numCore; i++) {
      queueArray.add(new LinkedList<>());
    }
    threadArray = new ArrayList<>();

    for (int i = 0; i < numCore; i++) {
      threadArray.add(new BaseElasthread(String.valueOf(i), this));
    }
    collector = acollector;
    collectorQueue = new ConcurrentLinkedQueue<>();
    lock = new AtomicInteger(0);
    keyCountMap = new HashMap<>();
    keyThreadMap = new HashMap<>();
    // point first free thread to be 0
    nextFreeQueue = new LinkedList<>();
    nextFreeQueue.push(0);
  }

  public LinkedList<Tuple> getQueue(int i) {
    return queueArray.get(i);
  }

  public final void runBolt() {
    for (int i = 0; i < numCore; i++) {
      // for each of the "core" assigned which is represented by a thread, we check its queue to
      // see if it is empty, if its not empty, we create a new thread and run it
      if (!getQueue(i).isEmpty()) {
        if (threadArray.get(i) == null) {
          threadArray.add(new BaseElasthread(String.valueOf(i), this));
        }
        threadArray.get(i).start();
        // update the number of threads running at the moment
        lock.getAndIncrement();
      }
    }
    // printStateMap();
    while (!collectorQueue.isEmpty()) {
      BaseCollectorTuple next = collectorQueue.poll();
      collector.emit(next.getT(), new Values(next.getS()));
    }

    // waits for threads finish their jobs and to "join"
    while (lock.get() > 0) {
      continue;
    }
  }

  // emit tuples if the output queue is not empty
  public void checkQueue() {
    while (!collectorQueue.isEmpty()) {
      BaseCollectorTuple next = collectorQueue.poll();
      collector.emit(next.getT(), new Values(next.getS()));
    }
    // debug to print state if last check is > 15 second
    if (System.currentTimeMillis() - latency > 15000) {
      printStateMap();
    }
    latency = System.currentTimeMillis();
  }

  public void decrementLock() {
    lock.getAndDecrement();
  }

  public int getNumCore() {
    return numCore;
  }

  public void setNumCore(int numCore) {
    this.numCore = numCore;
  }

  public void loadTuples(Tuple t) {
    String key = t.getString(0);
    if (keyThreadMap.get(key) != null){ // check if key is already being processed
      queueArray.get(keyThreadMap.get(key)).add(t);
      keyCountMap.get(key).incrementAndGet();
    } else { //if not, just assign it to the next freeThread, if there is no free thread, just next
      int nextFree = nextFreeQueue.pop();
      queueArray.get(nextFree).add(t);
      keyThreadMap.put(key, nextFree);
      keyCountMap.put(key, new AtomicInteger(1));
      if (nextFreeQueue.isEmpty()){
        nextFreeQueue.push((nextFree+1)%this.numCore);
      }
    }
    // simplistic hashcode allocation (outdated)
//    queueArray.get(Math.abs(t.getString(0).hashCode()) % this.numCore).add(t);
  }

  // used by the various threads converge and load into the output queue
  public synchronized void loadOutputTuples(Tuple t, String s) {
    BaseCollectorTuple output = new BaseCollectorTuple(t, s);
    collectorQueue.add(output);
  }


  public synchronized void updateState(String tuple, Integer number) {
    if (stateMap.get(tuple) == null) {
      stateMap.put(tuple, number);
    } else {
      int amount = stateMap.get(tuple);
      stateMap.put(tuple, amount + number);
    }
  }

  public synchronized void updateLoadBalancer(String key){
    int numLeft = keyCountMap.get(key).decrementAndGet();
    // check the number of task left for this key
    if (numLeft <= 0){
      // if no more left, remove key and update hashmaps and nextFreeThread
      // avoiding priority queue for now to prevent extra calculation which might not help that much
      nextFreeQueue.push(keyThreadMap.get(key));


      keyCountMap.remove(key);
      keyThreadMap.remove(key);
    }
  }

  public void printStateMap() {
    System.out.println(Collections.singletonList(stateMap));
  }
}
