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
 *
 * The advantage of using an ElasticBolt is that it
 * 1. reduces the overhead of resources needed to otherwise create other heron instance to host
 * the duplicated bolts
 * 2. ElasticBolts are also elastic in the sense that it has the ability to scale up and down
 * the number of threads, hence cores which are used to process tuples without disrupting the
 * processing of tuples, in a regular bolt, one would need to run the heron update command which
 * temporarily halts processing of tuples. Which is disruptive to tuple processing
 * this is less responsive to the change as compared to ElasticBolt which has an API exposed to
 * allow the user to remotely scale up or down a bolt without halting tuple processing
 * 3. ElasticBolts are also tunable, using the upperThresh, upperThresh and sleepDuration the user
 * can choose to to make the bolt faster and more aggressive, to be able to have higher throughput
 *
 * However it is noteworthy that it is possible for the Elasticbolt to process the tuples so fast
 * and emit it such that it overwhelm the collector's output queue which will then crash the
 * bolt, it is advisable for the user to test experiment with their typical workload and
 * tune Elasticbolt accordingly to maximize speed yet at the same time not overloading the output
 * if we remove the check in the bolt instance to block
 */

public abstract class BaseElasticBolt extends BaseComponent implements IElasticBolt {
  private static final long serialVersionUID = 4309732999277305080L;

  private OutputCollector boltCollector;
  private long latency = System.currentTimeMillis();

  // Cores are the number of threads that are running
  private int numCore = -1; // numCore are the number of threads currently being used
  private int maxCore = -1; // maxCore are the number of threads the system has
  private int backPressureLowerThreshold = 50;
  private int backPressureUpperThreshold = 150;
  private int sleepDuration = 20;
  private boolean backPressure = false;

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
  private AtomicInteger outstandingTuples;
  public boolean debug = false;

  /**
   * Initialize the Elasticbolt with the required data structures and threads
   * based on the topology
   *
   * @param aCollector The acollector is the OutputCollector used by this bolt to emit tuples
   * downstream
   *
   */
  public void initElasticBolt(OutputCollector aCollector) {
    this.boltCollector = aCollector;
    queueArray = new ArrayList<>();
    threadArray = new ArrayList<>();
    loadArray = new ArrayList<>();
    inQueue = new LinkedList<>();
    stateMap = new ConcurrentHashMap<>();
    keyCountMap = new HashMap<>();
    keyThreadMap = new HashMap<>();
    outstandingTuples = new AtomicInteger(0);
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
      }
    }
    if (debug) {
      checkQueue();
    }
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
    outstandingTuples.incrementAndGet();
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
    outstandingTuples.decrementAndGet();
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
  }

  public void scaleDown(int cores) {
    int scale = Math.abs(cores); // sanity check to prevent "negative" down
    this.numCore = Math.max(1, this.numCore - scale);
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
    while (!inQueue.isEmpty()) {
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
  }

  // used by the various threads converge and synchroniously load into the output queue
  public synchronized void loadOutputTuples(Tuple t, Values v) {
    boltCollector.emit(t, v);
  }

  public void printStateMap() {
    System.out.println(Collections.singletonList(stateMap));
  }

  public int getSleepDuration() {
    return this.sleepDuration;
  }

  @Override
  public void cleanup() {
  }

  @Override
  public void execute(Tuple tuple) {
  }

  public int getNumOutStanding() {
    return outstandingTuples.get();
  }

  public void setSleepDuration(int newValue) {
    this.sleepDuration = newValue;
  }
}

