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

/**
 * Created by zhengyang on 18/10/17.
 */
public abstract class AutoElasticBolt extends BaseElasticBolt implements IElasticBolt  {
  private static final long serialVersionUID = 7993201650995318206L;

  // Tries to determine what is the optimal number of cores to be used to process the pending data
  // based on the number of keys
  // Find the max number of available cores that evenly divides the number of keys, if no such value
  // is found, default to the user defined number of cores allowed
  // optimal use case is where load is equal among keys

  public void runBolt() {
    // get the number of distinct keys for this iteration
    int numKey = getNumDistinctKeys();
    // get what is the max number of cores available is a min of (user definition , system resource)
    int newNumberOfCores = 0;
    int delta = 0;
    int bestRuns = Integer.MAX_VALUE;
    // find the core minimal number of runs required to finish processing data
    for (int i = 1; i <= getUserDefinedNumCore(); i++) {
      // stop once we found a match

      int numRuns = numKey / i;
      if (numKey % i != 0) {
        numRuns++;
      }
      if (numRuns < bestRuns) {
        bestRuns = numRuns;
        newNumberOfCores = i;
      }
    }

    // Fallback, we default to max number of cores allowed
    if (newNumberOfCores == 0) {
      newNumberOfCores = getUserDefinedNumCore();
    }

    // calculate the number of cores to scale up or down
    delta = this.getNumCore() - newNumberOfCores;

    if (getDebug()) {
      System.out.println(newNumberOfCores + "|" + this.getNumCore() + "|" + delta);
    }

    if (delta < 0) {
      scaleUp(Math.abs(delta));
    } else if (delta > 0) {
      scaleDown(delta);
    }

    super.runBolt();
  }
}
