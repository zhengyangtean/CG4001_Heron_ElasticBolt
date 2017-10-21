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
import java.util.Map;

import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

/**
 * When writing topologies using Java, {@link IRichBolt} and
 * {@link com.twitter.heron.api.spout.IRichSpout} are the main interfaces
 * to use to implement components of the topology.
 */
public interface IElasticBolt extends IRichBolt {

  // for users to set maximum number of core/threads used
  void setNumCore(int numCore);

  void setMaxCore(int maxCore);

  LinkedList<Tuple> getQueue(int i);

  void execute(Tuple tuple);

  void loadTuples(Tuple tupleToQueue);

  void runBolt();

  void initElasticBolt(OutputCollector boltCollector);

  void updateLoadBalancer(String key);

  /**
   * API for scaling up the number of cores being used
   * <p>
   * Limited by the number of actual core within the system
   */
  void scaleUp(int cores);

  /**
   * API for scaling down the number of cores being used
   * <p>
   * Limited by at least 1 core has to be used
   */
  void scaleDown(int cores);

  // for debugging/information
  void setDebug(Boolean debug);

  void loadOutputTuples(Tuple t, Values v);

  int getNumCore();

  int getMaxCore();

  void setSleepDuration(int newValue);

  int getSleepDuration();

  int getNumOutStanding();

  void printStateMap();


  int incrementAndGetState(String tuple, int number);

  int getState(String tuple);

  int getState(String tuple, int defaultValue);

  int getNumDistinctKeys();

  void putState(String tuple, int value);

  Map<String, Integer> getStateMap();

  void setStateMap(Map<String, Integer> map);

  void setMaxNumBatches(int numBatch);

  int getMaxNumBatches();

  int incrementAndGetNumBatch();

  int getNumWorkingKeys();

  void resetNumBatch();
}
