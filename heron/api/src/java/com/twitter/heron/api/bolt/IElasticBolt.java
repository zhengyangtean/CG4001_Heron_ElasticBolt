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
import java.util.concurrent.ConcurrentLinkedQueue;

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

  ConcurrentLinkedQueue<Tuple> getQueue(int i);

  void execute(Tuple tuple);

  void loadTuples(Tuple tupleToQueue);

  void runBolt();

  void initElasticBolt(OutputCollector collector);

  void updateState(String tuple, Integer number);

  void decrementLock();

  void checkQueue();

  void updateLoadBalancer(String key);

  void scaleUp(int cores);

  void scaleDown(int cores);

  void checkFreeze();

  // for debugging/information
  void setDebug(Boolean debug);

  void loadOutputTuples(Tuple t, Values v);

  int getNumCore();

  int getMaxCore();

  void printStateMap();
}
