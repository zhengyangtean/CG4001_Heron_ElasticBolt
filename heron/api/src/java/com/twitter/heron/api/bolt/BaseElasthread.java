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

import java.util.LinkedList;

import com.twitter.heron.api.tuple.Tuple;

/**
 * Created by zhengyang on 8/8/17.
 * Implementation of threads for ElasticBolt
 */
public class BaseElasthread implements IElasthread {
  private Thread thisThread;
  private String threadName;
  private final IElasticBolt parentBolt;

  BaseElasthread(String name, IElasticBolt parentBolt) {
    this.threadName = name;
    this.parentBolt = parentBolt;
  }

  public void run() {
    LinkedList<Tuple> q = parentBolt.getQueue(Integer.parseInt(threadName));
    while (!q.isEmpty()) {
      Tuple nextTuple = q.poll();
      parentBolt.execute(nextTuple);
      parentBolt.updateLoadBalancer(nextTuple.getString(0));
    }
    thisThread = null;
  }

  public void start() {
    if (thisThread == null) {
      thisThread = new Thread(this, threadName);
      thisThread.start();
    }
  }
}
