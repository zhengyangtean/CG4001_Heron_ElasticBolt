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
import java.util.Random;
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
public abstract class TwitchyElasticBolt extends BaseComponent implements IElasticBolt {
  private static final long serialVersionUID = -8986777904209608575L;
  private int numCore = -1;
  private int maxCore = -1;
  private int twitchyness = 3;
  private Random rng;
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
  private int nextFree;

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

    // initialize all but they might not be used.
    for (int i = 0; i < this.maxCore; i++) {
      queueArray.add(new LinkedList<>());
    }
    threadArray = new ArrayList<>();
    for (int i = 0; i < this.maxCore; i++) {
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
    rng = new Random();
    nextFree = 0;
  }

  public LinkedList<Tuple> getQueue(int i) {
    return queueArray.get(i);
  }

  private void twitch(){
    // 50% of the chance to scale up or down
    int amt = Math.abs(rng.nextInt()%twitchyness);
    if (rng.nextInt()%2 == 0){
      // go up by [0,twitchyness)
      scaleUp(amt);
    } else {
      // go down [0,twitchyness)
      scaleDown(amt);
    }
  }

  public final void runBolt() {
    System.out.println("HELLO::RUNNING BOLT ::" + this.numCore);
    for (int i = 0; i < this.numCore; i++) {
      // for each of the "core" assigned which is represented by a thread, we check its queue to
      // see if it is empty, if its not empty, we create a new thread and run it
      if (!getQueue(i).isEmpty()) {
        if (threadArray.get(i) == null) {
          System.out.println("HELLO::ADDING_"+i);
          threadArray.add(new BaseElasthread(String.valueOf(i), this));
        }
        System.out.println("HELLO::STARTING_"+i);
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
    twitch();
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
    return this.numCore;
  }

  public void setNumCore(int numCore) {
    this.numCore = numCore;
  }

  public void setMaxCore(int maxCore) {
    this.maxCore = maxCore;
  }

  public int getMaxCore() {
    return this.maxCore;
  }

  public void loadTuples(Tuple t) {
//    // simplistic hashcode allocation (outdated)
//    queueArray.get(Math.abs(t.getString(0).hashCode()) % this.numCore).add(t);
    String key = t.getString(0);
    if (keyThreadMap.get(key) != null){ // check if key is already being processed
      queueArray.get(keyThreadMap.get(key)).add(t);
      keyCountMap.get(key).incrementAndGet();
    } else { //if not, just assign it to the next freeThread, if there is no free thread, just next
      nextFree %= this.numCore; // safety measure incase scale down
      int freeNode = nextFree;
      LinkedList<Tuple> queue = queueArray.get(freeNode);
      int freeNodeLoad = queue.size();
      if (this.numCore > 1) {
        for (int i = 1; i < this.numCore; i++) {
          int testNode = (nextFree + i)% this.numCore;
          System.out.println("DEBUG::LOADTUPLES::"+testNode+"::"+"|i_size:"+queue.size()+"|min"+ freeNodeLoad);
          queue = queueArray.get(testNode);
          if (queue.size() <= freeNodeLoad){
            freeNodeLoad = queue.size();
            freeNode = testNode;
          }
        }
        nextFree++;
      }
      keyThreadMap.put(key, freeNode);
      keyCountMap.put(key, new AtomicInteger(1));
    }
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
      keyCountMap.remove(key);
      keyThreadMap.remove(key);
    }
  }

  public synchronized void scaleUp(int cores){
    if  (this.numCore + cores > this.maxCore){
      this.numCore = this.maxCore;
    } else {
      this.numCore += cores;
    }
  }

  public synchronized void scaleDown(int cores){
    if  (this.numCore - cores <= 0){
      this.numCore = 1;
    } else {
      this.numCore -= cores;
    }
  }

  public void printStateMap() {
    System.out.println(Collections.singletonList(stateMap));
  }
}
