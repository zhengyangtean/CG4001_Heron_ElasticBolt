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
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhengyang on 22/10/17.
 */
public abstract class DFFElasticBolt extends BaseElasticBolt implements IElasticBolt  {
  private static final long serialVersionUID = 2567533972012329000L;

  public void runBolt() {

    // sort HM to get keys by descending load order
    HMcomparator HMsorter = new HMcomparator(this.currentDistinctKeys);
    TreeMap<String, Integer> descendingValueMap = new TreeMap<String, Integer>(HMsorter);

    // target capacity where all tuples are evenly divided
    int targetCapacity = (int)(Math.ceil(getNumOutStanding()*1.0/getNumCore()));

    // initialize core capacity to target capacity
    ArrayList<Integer> coreCapacity = new ArrayList<>(getNumCore());
    for (int i = 0; i < getNumCore(); i++){
      coreCapacity.add(targetCapacity);
    }

    // Assign Core using modififed DFF algo
    for(String key: descendingValueMap.keySet()){
      boolean added = false;
      int keySize = descendingValueMap.get(key);
      // find to see if can fit into any core
      for (int i = 0; i < getNumCore(); i++){
        if (coreCapacity.get(i) > keySize) {
          coreCapacity.set(i, coreCapacity.get(i)-keySize);
          keyThreadMap.put(key, i);
          keyCountMap.put(key, new AtomicInteger(1));
          added = true;
          break;
        }
      }
      // if cant fit into any core, find the least loaded/used core and assign it to the core
      if (!added){
        int maxCapacityCore = getMaxCapacityCore(coreCapacity);
        coreCapacity.set(maxCapacityCore, coreCapacity.get(maxCapacityCore)-keySize);
        keyThreadMap.put(key, maxCapacityCore);
        keyCountMap.put(key, new AtomicInteger(0));
      }
    }

    super.runBolt();
  }

  // Gets the minimal loaded core give a list of core loads
  private int getMaxCapacityCore(ArrayList<Integer> coreCapacity) {
    int maxCapacity = coreCapacity.get(0);
    int maxCapacityCore = 0;
    int capacity;
    for (int i = 1 ; i < coreCapacity.size(); i++){
      capacity = coreCapacity.get(i);
      if (capacity > maxCapacity) {
        maxCapacityCore = i;
        maxCapacity = capacity;
      }
    }
    return maxCapacityCore;
  }

  class HMcomparator implements Comparator<String> {
    Map<String, Integer> originalMap;

    public HMcomparator(Map<String, Integer> originalMap) {
      this.originalMap = originalMap;
    }

    public int compare(String a, String b) {
      if (originalMap.get(a) >= originalMap.get(b)) {
        return -1;
      } else {
        return 1;
      }
    }
  }

}


