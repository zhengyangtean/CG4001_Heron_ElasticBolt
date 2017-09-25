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
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.twitter.heron.api.topology.BaseComponent;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

/**
 * Created by zhengyang on 25/6/17.
 * Assumptions made is that the key for the state are strings and the values and integers
 */
public abstract class TwitchyElasticBolt extends BaseElasticBolt implements IElasticBolt {
  private static final long serialVersionUID = -8986777904209608575L;
  private int twitchyness = 3;
  private int twitchProbability = 3;
  private boolean freeze;
  private Random rng;

  public void initElasticBolt(OutputCollector acollector){
    super.initElasticBolt(acollector);
    rng = new Random();
  }

  private void twitch(){
    int amt = Math.abs(rng.nextInt(10));
    if (amt < twitchProbability){ // twitchProbability of twitching
      amt = Math.abs(rng.nextInt()%twitchyness);
      if (rng.nextInt()%2 == 0){     // 50% of the chance to scale up or down
        // go up by [0,twitchyness)
        System.out.println("SCALING_UP :: " + amt);
        scaleUp(amt);
        freeze = true;
      } else {
        // go down [0,twitchyness)
        System.out.println("SCALING_DOWN :: " + amt);
        scaleDown(amt);
      }
    }
  }

  public final void runBolt() {
    super.runBolt();
    twitch();
  }
}
