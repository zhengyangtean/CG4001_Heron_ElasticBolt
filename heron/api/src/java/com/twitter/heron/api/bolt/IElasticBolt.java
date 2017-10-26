// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.api.bolt;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

/**
 * When writing topologies using Java, elastic alternative to {@link IRichBolt}
 */
public interface IElasticBolt extends IRichBolt {

  /**
   * Initialize the Elasticbolt with the required data structures and threads
   * based on the topology
   * <p>
   * @param boltCollector The acollector is the OutputCollector used by this bolt to emit tuples
   * downstream
   */
  void initElasticBolt(OutputCollector boltCollector);

  /**
   * Ingress for boltInstance to call to start processing tuples after collecting sufficient tuples
   */
  void runBolt();

  /**
   * Allow the implementation of IElasticthread to retrieve the queue of tuples which it is
   * supposed to process
   * <p>
   * @param queueIndex the index of which the queue which is being retrieved
   * @return queue of Tuples for processing
   */
  LinkedList<Tuple> getQueue(int queueIndex);

  /**
   * Query number of cores which are assigned tuples to process
   * <p>
   * @return  the number of cores
   */
  int getNumCore();

  /**
   * Set the number of cores which are assigned tuples to process
   * <p>
   * @param numCore the index of which the queue which is being retrieved
   */
  void setNumCore(int numCore);

  /**
   * Query max number of cores which can be assigned to process tuples
   * <p>
   * @return  the maximum number of cores based on current system
   */
  int getMaxCore();

  /**
   * Set the maximum number of cores which can be assigned to process tuples in this topology
   * based on the number of cores available to Java
   * <p>
   * @param maxCore the index of which the queue which is being retrieved
   */
  void setMaxCore(int maxCore);

  /**
   * Using User defined logic to process tuple and enqueue it for emitting if needed
   * <p>
   * @param tuple to process
   */
  void execute(Tuple tuple);

  /**
   * Allow boltInstance to enqueue tuples from upstream into bolt, pending processing
   * assumes that the first value of the tuple is a string and is the key of the tuple
   * <p>
   * @param tuple to be executed/processed by the bolt
   */
  void loadTuples(Tuple tuple);

  /**
   * Called from Elasticthread, update ElasticBolt's datastructures that a tuple of given key has
   * been processed, synchronized due to the multithreade nature of Elasticthread writing to a
   * single instance of ElasticBolt
   * <p>
   * @param key to update
   */
  void updateLoadBalancer(String key);

  /**
   * API for scaling up the number of cores being used
   * Has sanity check for "negative" increment also checks if Java has sufficient cores
   * which is stored in maxCore, numCore is bounded by [1, maxCore]
   * <p>
   * @param cores number of cores to increase
   */
  void scaleUp(int cores);

  /**
   * API for scaling down the number of core being used
   * Has sanity check for "negative" decrement also checks if Java numCore < 1
   * numCore is bounded by [1, maxCore]
   * <p>
   * @param cores number of cores to increase
   */
  void scaleDown(int cores);

  /**
   * Collector is NOT threadsafe
   * Since we are multithreading the execution of tuples we need to join it before handing back to
   * boltCollector for emitting to downstream processing (if needed) hence the need
   * for synchronizing
   * <p>
   * @param tuple anchoring tuple
   * @param value the new value to emit
   */
  void emitTuple(Tuple tuple, Values value);

  /**
   * get the number of batches of tuples to aggregate from upstream before processing
   * <p>
   * @return  max number of batches to aggregate
   */
  int getMaxNumBatches();

  /**
   * Set the number of batches of tuples to aggregate from upstream before processing
   * <p>
   * @param numBatch number of batches
   */
  void setMaxNumBatches(int numBatch);

  /**
   * increment and track the current batch of tuples being aggregated
   * <p>
   * @return the current number of batches aggregated
   */
  int incrementAndGetNumBatch();

  /**
   * resets the number of batches that have been aggregated
   * used when we have finish aggregating and started processing tuples
   */
  void resetNumBatch();

  /**
   * Check the amount of time that boltInstance is to sleep while bolt executing tuples before
   * checking to see if bolt have finish executing tuples
   * <p>
   * @return amount of time to sleep
   */
  int getSleepDuration();

  /**
   * Set the amount of time that boltInstance is to sleep while executing tuples
   * <p>
   * @param timeToSleep the amount of time to sleep at one go
   */
  void setSleepDuration(int timeToSleep);

  /**
   * Get the number of distinct keys of the pending tuples
   * <p>
   * @return  number of distinct keys
   */
  int getNumDistinctKeys();

  /**
   * Dynamically check the number of outstanding tuples which are still in the process of processing
   * <p>
   * @return  number of outstanding tuples
   */
  int getNumOutStanding();

  /**
   * Dynamically check the number of outstanding keys which are still in the process of processing
   * <p>
   * @return  number of keys still being processed
   */
  int getNumWorkingKeys();

  /**
   * Get the numCore set by the user
   * <p>
   * @return  numCore set by the user
   */
  int getUserDefinedNumCore();

  /**
   * Set the numCore set by the user
   * <p>
   * @param cores set by user
   */
  void setUserDefinedNumCore(int cores);

  /**
   * Set whether to should the bolt run in debug mode
   * <p>
   * @param debug should debug actions like printing be performed
   */
  void setDebug(Boolean debug);

  /**
   * Increment the state of the key by amountToIncrease
   * <p>
   * @param key key to increment
   * @param amountToIncrease
   */
  int incrementAndGetState(String key, int amountToIncrease);

  /**
   * Decrement the state of the key by amountTODecrease
   * <p>
   * @param key key to increment
   * @param amountToDecrease
   */
  int decrementAndGetState(String key, int amountToDecrease);

  /**
   * get state of key
   * <p>
   * @param key
   */
  int getState(String key);

  /**
   * get state of key
   * <p>
   * @param key
   * @param defaultValue value to default to if key is not found in state maps
   */
  int getState(String key, int defaultValue);

  /**
   * write state of the key by amountToDecrease
   * <p>
   * @param key key to increment
   * @param value
   */
  void putState(String key, int value);

  /**
   * Allows retrieval of the current state
   * <p>
   * @return the state map
   */
  ConcurrentHashMap<String, Integer> getStateMap();

  /**
   * Allows overwriting of the current state
   * <p>
   * @param map to overwrite current state map
   */
  void setStateMap(ConcurrentHashMap<String, Integer> map);
}
