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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.twitter.heron.api.utils.HMcomparator;

/**
 * Created by zhengyang on 22/10/17.
 */
public abstract class FFDElasticBolt extends BaseElasticBolt implements IElasticBolt  {
  private static final long serialVersionUID = -2730620454868078925L;

  /**
   * Extends the original runBolt function of Elasticbolt to step in and assign the keys to
   * execution queues based on the tuple statistics , calls super.runBolt upon complete assignment
   */
  public void runBolt() {

    // HM (O(1))is used initially for for quick access to tuples and count
    // TM (O(log n) itself couldnt help as it is sorted on key not on value
    // hence the round about way to sort
    if (getDebug()) {
      System.out.println("FFD ::Starting");
    }

    // sort HM to get keys by descending load order
    HMcomparator hmSorter = new HMcomparator(getPendingKeyCountMap());
    TreeMap<String, Integer> descendingValueMap = new TreeMap<>(hmSorter);
    descendingValueMap.putAll(getPendingKeyCountMap());

    // target capacity where all tuples are evenly divided
    int targetCapacity = (int) (Math.ceil(getNumOutStanding() * 1.0 / getNumCore()));

    if (getDebug()) {
      System.out.println("FFD ::numOutstanding: " + Integer.toString(getNumOutStanding()));
      System.out.println("FFD ::numcore       : " + Integer.toString(getNumCore()));
      System.out.println("FFD ::targetCapacity: " + Integer.toString(targetCapacity));
      System.out.println(getPendingKeyCountMap());
      System.out.println(getPendingKeyCountMap().keySet());
      System.out.println(descendingValueMap.keySet());
    }

    // initialize core capacity to target capacity
    ArrayList<Integer> coreCapacity = new ArrayList<>(getNumCore());
    for (int i = 0; i < getNumCore(); i++) {
      coreCapacity.add(targetCapacity);
    }

    HashMap<String, Integer> keyThreadMap = getKeyThreadMap();
    HashMap<String, AtomicInteger> keyCountMap = getKeyCountMap();

    // Assign Core using modififed DFF algo
    for (String key: descendingValueMap.keySet()) {
      boolean added = false;
      int keySize = getPendingKeyCountMap().get(key);
      // find to see if can fit into any core
      for (int i = 0; i < getNumCore(); i++) {
        if (coreCapacity.get(i) > keySize) {
          coreCapacity.set(i, coreCapacity.get(i) - keySize);
          keyThreadMap.put(key, i);
          keyCountMap.put(key, new AtomicInteger(1));
          added = true;
          System.out.println(i + "|" +  keySize);
          break;
        }
      }
      // if cant fit into any core, find the least loaded/used core and assign it to the core
      if (!added) {
        int maxCapacityCore = getMaxCapacityCore(coreCapacity);
        coreCapacity.set(maxCapacityCore, coreCapacity.get(maxCapacityCore) - keySize);
        keyThreadMap.put(key, maxCapacityCore);
        keyCountMap.put(key, new AtomicInteger(0));
        System.out.println(maxCapacityCore + "|" +  keySize);
      }
    }

    super.runBolt();
  }

  // Gets the minimal loaded core give a list of core loads
  private int getMaxCapacityCore(ArrayList<Integer> coreCapacity) {
    int maxCapacity = coreCapacity.get(0);
    int maxCapacityCore = 0;
    int capacity;
    for (int i = 1; i < coreCapacity.size(); i++) {
      capacity = coreCapacity.get(i);
      if (capacity > maxCapacity) {
        maxCapacityCore = i;
        maxCapacity = capacity;
      }
    }
    return maxCapacityCore;
  }
}


