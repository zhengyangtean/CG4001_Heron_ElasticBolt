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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.twitter.heron.api.topology.BaseComponent;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

/**
 * Created by zhengyang on 25/6/17.
 * The advantage of using an ElasticBolt:
 * 1. Dynamically scalable Bolt
 * 2. Avoids costly synchronisation upon keyspace repartitioning
 * 3. Reduces overhead of parallel Bolts on same container
 * 4. Compatible with field grouping
 */
public abstract class BaseElasticBolt extends BaseComponent implements IElasticBolt {
  private static final long serialVersionUID = 1643680957503810571L;
  private OutputCollector collector;

  private int sleepDuration = 20; // decide the sleep duration during checks by boltinstance
  private boolean debug = false;   // To decide to print debug lines or not

  /**
   * Cores are the number of threads that are running
   **/
  private int numCore = -1; // numCore are the number of threads currently being used
  private int maxCore = -1; // maxCore are the number of threads the system has
  private int userDefinedNumCore = -1; // maxCore as defined by user

  /**
   * Various data structure to keep track of the load for sharding and scaling purpose
   * ArrayList as the external list of the number of queues are constant (added only at init)
   * LinkedList as the internal queue due to its better add/remove performance
   *
   * note : pending tuples are tuples that are in inqueue but not sharded yet
   **/
  // Serves as the the single point of entry of in-buffer for this elasticbolt
  protected LinkedList<Tuple> inQueue; // LL due to its high add and remove frequency
  // Serves as the map of queue to feed tuples into various threads
  protected ArrayList<LinkedList<Tuple>> queueArray;
  // Keeps track of the threads in this ElasticBolt
  private ArrayList<BaseElasthread> threadArray;
  // Serve as the in-memory state
  private ConcurrentHashMap<String, Integer> stateMap;

  // Map key to number of tuples that is yet to be processed
  private HashMap<String, AtomicInteger> keyCountMap;
  // Map key to which thread it is currently assigned to
  private HashMap<String, Integer> keyThreadMap;
  // Map number of keys assigned to each node (index)
  private ArrayList<AtomicInteger> loadArray;
  // Map key to the number of pending tuple with said key
  private HashMap<String, Integer> pendingKeyCountMap;
  // Total number of tuples that is still within the Bolt
  private AtomicInteger outstandingTuples;

  /**
   * Initialize the Elasticbolt with the required data structures and threads
   * based on the topology
   * <p>
   * @param boltCollector The acollector is the OutputCollector used by this bolt to emit tuples
   * downstream
   */
  public void initElasticBolt(OutputCollector boltCollector) {
    this.collector = boltCollector;
    // initialize data structures
    queueArray = new ArrayList<>();
    threadArray = new ArrayList<>();
    loadArray = new ArrayList<>();
    inQueue = new LinkedList<>();
    keyCountMap = new HashMap<>();
    keyThreadMap = new HashMap<>();
    pendingKeyCountMap = new HashMap<>();
    stateMap = new ConcurrentHashMap<>();
    outstandingTuples = new AtomicInteger(0);
    for (int i = 0; i < this.maxCore; i++) {
      queueArray.add(new LinkedList<>());
      threadArray.add(new BaseElasthread(String.valueOf(i), this));
      loadArray.add(new AtomicInteger(0));
    }
  }

  /**
   * Ingress for boltInstance to call to start processing tuples after collecting sufficient tuples
   */
  public void runBolt() {
    runBoltHook();
    shardTuples();
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
  }

  /**
   * Allow the implementation of IElasticthread to retrieve the queue of tuples which it is
   * supposed to process
   * <p>
   * @param queueIndex the index of which the queue which is being retrieved
   * @return queue of Tuples for processing
   */
  public LinkedList<Tuple> getQueue(int queueIndex) {
    return queueArray.get(queueIndex);
  }

  /**
   * Query number of cores which are assigned tuples to process
   * <p>
   * @return  the number of cores
   */
  public int getNumCore() {
    return numCore;
  }

  /**
   * Set the number of cores which are assigned tuples to process
   * <p>
   * @param numCore the index of which the queue which is being retrieved
   */
  public void setNumCore(int numCore) {
    this.numCore = numCore;
  }

  /**
   * Query max number of cores which can be assigned to process tuples
   * <p>
   * @return  the maximum number of cores based on current system
   */
  public int getMaxCore() {
    return maxCore;
  }

  /**
   * Set the maximum number of cores which can be assigned to process tuples in this topology
   * based on the number of cores available to Java
   * <p>
   * @param maxCore the index of which the queue which is being retrieved
   */
  public void setMaxCore(int maxCore) {
    this.maxCore = maxCore;
  }



  /**
   * Allow boltInstance to enqueue tuples from upstream into bolt, pending processing
   * assumes that the first value of the tuple is a string and is the key of the tuple
   * <p>
   * @param tuple to be executed/processed by the bolt
   */
  public void loadTuples(Tuple tuple) {
    inQueue.add(tuple);
    if (pendingKeyCountMap.containsKey(tuple.getString(0))) {
      pendingKeyCountMap.put(tuple.getString(0), pendingKeyCountMap.get(tuple.getString(0)) + 1);
    } else {
      pendingKeyCountMap.put(tuple.getString(0), 1);
    }
    outstandingTuples.incrementAndGet();
  }

  /**
   * Called from Elasticthread, update ElasticBolt's datastructures that a tuple of given key has
   * been processed, synchronized due to the multithreade nature of Elasticthread writing to a
   * single instance of ElasticBolt
   * <p>
   * @param key to update
   */
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

  /**
   * API for scaling up the number of cores being used
   * Has sanity check for "negative" increment also checks if Java has sufficient cores
   * which is stored in maxCore, numCore is bounded by [1, maxCore]
   * <p>
   * @param cores number of cores to increase
   */
  public void scaleUp(int cores) {
    int scale = Math.abs(cores); // sanity check to prevent "negative" scaleup
    this.numCore = Math.min(this.maxCore, this.numCore + scale);
  }

  /**
   * API for scaling down the number of core being used
   * Has sanity check for "negative" decrement also checks if Java numCore < 1
   * numCore is bounded by [1, maxCore]
   * <p>
   * @param cores number of cores to increase
   */
  public void scaleDown(int cores) {
    int scale = Math.abs(cores); // sanity check to prevent "negative" down
    this.numCore = Math.max(1, this.numCore - scale);
  }

  /**
   * Find the least loaded core in terms of number of keys assigned to core,
   * which is the default load balancing algorithm
   */
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
    return leastLoadedNode;
  }

  /**
   * Shard all pending tuples into respective core queues for processing
   */
  protected void shardTuples() {
    while (!inQueue.isEmpty()) {
      Tuple t = inQueue.poll();
      if (t == null) {
        if (debug) {
          System.out.println("WARNING :: NULL TUPLE");
        }
        continue;
      }
      String key = t.getString(0);
      if (key == null) {
        if (debug) {
          System.out.println("WARNING :: NULL/INVALID TUPLE KEY");
        }
        continue;
      }
      if (keyThreadMap.get(key) != null) { // check if key is already being processed
        queueArray.get(keyThreadMap.get(key)).add(t);
        keyCountMap.get(key).incrementAndGet();
      } else { //if not, just assign it to the next least loaded node
        int nextFreeNode = getLeastLoadedNode();
        if (debug) {
          System.out.println("ASSIGNING :: " + key + " <to> " + nextFreeNode);
        }
        // update tracking datastructures
        loadArray.get(nextFreeNode).getAndIncrement();
        queueArray.get(nextFreeNode).add(t);
        keyThreadMap.put(key, nextFreeNode);
        keyCountMap.put(key, new AtomicInteger(1));
      }
    }
    // since tuples are already sharded they are no longer "pending"
    pendingKeyCountMap.clear();
  }

  /**
   * Collector is NOT threadsafe
   * Since we are multithreading the execution of tuples we need to join it before handing back to
   * boltCollector for emitting to downstream processing (if needed) hence the need
   * for synchronizing
   * <p>
   * @param tuple anchoring tuple
   * @param value the new value to emit
   */
  public synchronized void emitTuple(Tuple tuple, Values value) {
    collector.emit(tuple, value);
  }

  /**
   * Check the amount of time that boltInstance is to sleep while bolt executing tuples before
   * checking to see if bolt have finish executing tuples
   * <p>
   * @return amount of time to sleep
   */
  public int getSleepDuration() {
    return this.sleepDuration;
  }

  /**
   * Set the amount of time that boltInstance is to sleep while executing tuples
   * <p>
   * @param timeToSleep the amount of time to sleep at one go
   */
  public void setSleepDuration(int timeToSleep) {
    this.sleepDuration = timeToSleep;
  }

  /**
   * Get the number of distinct keys of the pending tuples
   * <p>
   * @return  number of distinct keys
   */
  public int getNumDistinctKeys() {
    return pendingKeyCountMap.size();
  }

  /**
   * Dynamically check the number of outstanding tuples which are still in the process of processing
   * <p>
   * @return  number of outstanding tuples
   */
  public int getNumOutStanding() {
    return outstandingTuples.get();
  }

  /**
   * Dynamically check the number of outstanding keys which are still in the process of processing
   * <p>
   * @return  number of keys still being processed
   */
  public int getNumWorkingKeys() {
    return this.keyCountMap.size();
  }

  /**
   *  Get the pendingKeyCountMap
   *  @return keyCountMap
   */
  protected HashMap<String, Integer> getPendingKeyCountMap() {
    return this.pendingKeyCountMap;
  }

  /**
   *  Get the getPendingKeyCountMap
   *  @return getPendingKeyCountMap
   */
  protected HashMap<String, AtomicInteger> getKeyCountMap() {
    return this.keyCountMap;
  }

  /**
   *  Get the keyThread map
   *  @return keyCountMap
   */
  protected HashMap<String, Integer> getKeyThreadMap() {
    return this.keyThreadMap;
  }

  /**
   * Get the numCore set by the user
   * <p>
   * @return  numCore set by the user
   */
  public int getUserDefinedNumCore() {
    return userDefinedNumCore;
  }

  /**
   * Set the numCore set by the user
   * <p>
   * @param cores set by user
   */
  public void setUserDefinedNumCore(int cores) {
    userDefinedNumCore = cores;
  }

  /**
   * Hook to run function before runBolt, to be overwritten by child classes
   */
  public void runBoltHook() {

  }

  /**
   * check to see whether to should the bolt run in debug mode
   * <p>
   * @return  boolean to see if should do debug actions
   */
  public boolean getDebug() {
    return this.debug;
  }

  /**
   * Set whether to should the bolt run in debug mode
   * <p>
   * @param debug should debug actions like printing be performed
   */
  public void setDebug(Boolean debug) {
    this.debug = debug;
  }

  /**
   * Built in state to allow users to easily use state, majority of them are synchronized for
   * concurrency reason due to the multithreaded nature of ElasticBolt
   */

  /**
   * Increment the state of the key by amountToIncrease
   * <p>
   * @param key key to increment
   * @param amountToIncrease
   */
  public synchronized int incrementAndGetState(String key, int amountToIncrease) {
    if (stateMap.get(key) == null) {
      stateMap.put(key, amountToIncrease);
      return amountToIncrease;
    } else {
      int amount = stateMap.get(key);
      stateMap.put(key, amount + amountToIncrease);
      return amount + amountToIncrease;
    }
  }

  /**
   * Decrement the state of the key by amountToDecrease
   * <p>
   * @param key key to increment
   * @param amountToDecrease
   */
  public synchronized int decrementAndGetState(String key, int amountToDecrease) {
    if (stateMap.get(key) == null) {
      stateMap.put(key, amountToDecrease);
      return amountToDecrease;
    } else {
      int amount = stateMap.get(key);
      stateMap.put(key, amount - amountToDecrease);
      return amount + amountToDecrease;
    }
  }

  /**
   * get state of key
   * <p>
   * @param key
   */
  public synchronized int getState(String key) {
    return stateMap.get(key);
  }

  /**
   * get state of key
   * <p>
   * @param key
   * @param defaultValue value to default to if key is not found in state maps
   */
  public synchronized int getState(String key, int defaultValue) {
    if (stateMap.get(key) == null) {
      return defaultValue;
    }
    return stateMap.get(key);
  }

  /**
   * write state of the key by amountToDecrease
   * <p>
   * @param key key to increment
   * @param value
   */
  public synchronized void putState(String key, int value) {
    stateMap.put(key, value);
  }

  /**
   * Allows retrieval of the current state
   * <p>
   * @return the state map
   */
  public ConcurrentHashMap<String, Integer> getStateMap() {
    return stateMap;
  }

  /**
   * Allows overwriting of the current state
   * <p>
   * @param map to overwrite current state map
   */
  public void setStateMap(ConcurrentHashMap<String, Integer> map) {
    this.stateMap = map;
  }

  @Override
  public void cleanup() {
  }
}



