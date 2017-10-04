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
import java.util.concurrent.atomic.AtomicInteger;

import com.twitter.heron.api.topology.BaseComponent;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;

/**
 * Created by zhengyang on 25/6/17.
 * Assumptions made
 * > key for the state are strings and the values and integers
 * > Tuple have key String at index 0, ie. tuple.getString(0) returns the tuple's key
 * > unbounded stream of incoming tuples from upstream bolt/sprout
 */

public abstract class BaseElasticBolt extends BaseComponent implements IElasticBolt {
  private static final long serialVersionUID = 4309732999277305080L;
  private static final int backPressureLowerThreshold = 500;
  private static final int backPressureUpperThreshold = 1500;


  private OutputCollector collector;
  private long latency = System.currentTimeMillis();

  // Cores are the number of threads that are running
  private int numCore = -1; // numCore are the number of threads currently being used
  private int maxCore = -1; // maxCore are the number of threads the system has

  // this is used to synchronize/join all the threads in one iteration of processing
  private AtomicInteger lock;


  /**
   * ArrayList as the external list of the number of queues are constant (added only at init)
   * LinkedList as the internal queue due to its better add/remove performance
   **/
  // Serves as the the single point of entry of in-buffer for this elastibolt
  private LinkedList<Tuple> inQueue; // LL due to its high add and remove frequency
  // Serves as the map of queue to feed tuples into various threads
  private ArrayList<LinkedList<Tuple>> queueArray;
  // Keeps track of the threads in this ElasticBolt
  private ArrayList<BaseElasthread> threadArray;

  public Boolean freeze;

  /**
   * As the collector is not threadsafe, this concurrent queue is
   * meant to join all the tuple outputs from various threads into a single threaded queue for the
   * queue to process
   **/
  private ConcurrentHashMap<String, Integer> stateMap; // Serve as the inmemorystate

  /**
   * Various data structure to keep track of the load for sharding and scaling purpose
   */
  private HashMap<String, AtomicInteger> keyCountMap;
  private HashMap<String, Integer> keyThreadMap;
  private ArrayList<AtomicInteger> loadArray;
  public boolean debug = false;

  /**
   * Initialize the Elasticbolt with the required data structures and threads
   * based on the topology
   *
   * @param acollector The acollector is the OutputCollector used by this bolt to emit tuples
   * downstream
   */
  public void initElasticBolt(OutputCollector acollector) {
    collector = acollector;
    queueArray = new ArrayList<>();
    threadArray = new ArrayList<>();
    loadArray = new ArrayList<>();
    inQueue = new LinkedList<>();
    stateMap = new ConcurrentHashMap<>();
    lock = new AtomicInteger(0);
    keyCountMap = new HashMap<>();
    keyThreadMap = new HashMap<>();
    freeze = false;
    for (int i = 0; i < this.maxCore; i++) {
      queueArray.add(new LinkedList<>());
      threadArray.add(new BaseElasthread(String.valueOf(i), this));
      loadArray.add(new AtomicInteger(0));
    }
  }

  public void runBolt() {
    for (int i = 0; i < numCore; i++) {
      // for each of the "core" assigned which is represented by a thread, we check its queue to
      // see if it is empty, if its not empty, and thread
      // is null we create a new thread and run it
      if (!queueArray.get(i).isEmpty()) {
        if (threadArray.get(i) == null) {
          threadArray.add(new BaseElasthread(String.valueOf(i), this));
        }
        threadArray.get(i).start();
        // update the number of threads running at the moment
        lock.getAndIncrement();
      }
    }
    int numOutStanding = numOutStanding();
    if (numOutStanding >= backPressureUpperThreshold){
      while (true){
        if (numOutStanding() > backPressureLowerThreshold){
          Utils.sleep(10); // sleep for a while if there are too many outstanding tuples
        }
      }
    }
    checkQueue();
  }
  // emit tuples if the output queue is not empty
  // done by boltinstance before runbolt
  public synchronized void checkQueue() {
    // debug to print state if last check is > 15 second
    if (debug && System.currentTimeMillis() - latency > 15000) {
      printStateMap();
      latency = System.currentTimeMillis();
    }
  }

  public LinkedList<Tuple> getQueue(int i) {
    return queueArray.get(i);
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

  public void setMaxCore(int maxCore) {
    this.maxCore = maxCore;
  }

  public int getMaxCore() {
    return maxCore;
  }

  public void setDebug(Boolean debug) {
    this.debug = debug;
  }

  public synchronized void loadTuples(Tuple t) {
    inQueue.add(t);
    shardTuples();
  }

  public synchronized void updateState(String tuple, Integer number) {
    if (stateMap.get(tuple) == null) {
      stateMap.put(tuple, number);
    } else {
      int amount = stateMap.get(tuple);
      stateMap.put(tuple, amount + number);
    }
  }

  public synchronized void updateLoadBalancer(String key) {
    int node = keyThreadMap.get(key);
    int numLeft = keyCountMap.get(key).decrementAndGet();
    if (numLeft <= 0) {
      // if there is no more tuples of this key left
      // update the number of keys assigned to this node
      loadArray.get(node).decrementAndGet();
      keyCountMap.remove(key); // remove the mapping for key-tuplesleft
      keyThreadMap.remove(key); // remove the mapping for key-node
    }
  }

  public void scaleUp(int cores) {
    int scale = Math.abs(cores); // sanity check to prevent "negative" scaleup
    this.numCore = Math.min(this.maxCore, this.numCore + scale);
    this.freeze = true;
  }

  public void scaleDown(int cores) {
    int scale = Math.abs(cores); // sanity check to prevent "negative" down
    this.numCore = Math.max(1, this.numCore - scale);
    this.freeze = true;
  }

  private int getLeastLoadedNode() {
    int leastLoadedNode = 0;
    int leastLoad = loadArray.get(0).get();
    int currentLoad;
    for (int i = 1; i < this.numCore; i++) {
      currentLoad = loadArray.get(i).get();
      if (currentLoad <= leastLoad) {
        leastLoadedNode = i;
        leastLoad = currentLoad;
      }
    }
    // initially leastLoadedNode has no load, now it is no longer freeloading as it
    // is now going to be assigned a key
    loadArray.get(leastLoadedNode).getAndIncrement();
    return leastLoadedNode;
  }

  private void shardTuples() {
    // shard tuples into their thread queues if system is not frozen for migration
    if (!freeze) {
      if (!inQueue.isEmpty()) {
        Tuple t = inQueue.poll();
        if (!debug && t == null) {
          System.out.println("WARNING :: NULL TUPLE");
          return;
        }
        String key = t.getString(0);
        if (!debug && key == null) {
          System.out.println("WARNING :: NULL TUPLE KEY");
          return;
        }
        if (keyThreadMap.get(key) != null) { // check if key is already being processed
          queueArray.get(keyThreadMap.get(key)).add(t);
          keyCountMap.get(key).incrementAndGet();
        } else { //if not, just assign it to the next least loaded node
          int nextFreeNode = getLeastLoadedNode();
          if (debug) {
            System.out.println("ASSIGNING :: " + key + " <to> " + nextFreeNode);
          }
          queueArray.get(nextFreeNode).add(t);
          keyThreadMap.put(key, nextFreeNode);
          keyCountMap.put(key, new AtomicInteger(1));
        }
      }
    } else {
      checkFreeze();
    }
//    else {
//      System.out.println("::FROZEN:: " + this.numCore + "/" + this.maxCore);
//      System.out.println(loadArray + " :: " + this.inQueue.size());
//    }
  }

  // used by the various threads converge and synchroniously load into the output queue
  public synchronized void loadOutputTuples(Tuple t, Values v) {
    collector.emit(t, v);
  }

  public void printStateMap() {
    System.out.println(Collections.singletonList(stateMap));
  }

  public boolean getFreezeStatus() {
    return this.freeze;
  }

  @Override
  public void cleanup() {
  }

  @Override
  public void execute(Tuple tuple) {
  }

  public int numOutStanding(){
    int outstanding = 0;
    for (int i = 0; i < queueArray.size(); i++){
      outstanding += queueArray.get(i).size();
    }
    return outstanding;
  }

  // simplified the check free procedure
  public void checkFreeze() {

    System.out.println("~~:: " + numOutStanding() + " ::~~");
    // check to see if each queue is empty if all of them are empty unfreeze the system
    for (int i = 0; i < queueArray.size(); i++){
      if (!queueArray.get(i).isEmpty()){
        return;
      }
    }
    this.freeze = false;
    return;
  }
}

