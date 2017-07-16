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
import java.util.LinkedList;

import com.twitter.heron.api.topology.BaseComponent;
import com.twitter.heron.api.tuple.Tuple;

/**
 * Created by zhengyang on 25/6/17.
 */
public abstract class BaseElasticBolt extends BaseComponent implements IElasticBolt {
  private static final long serialVersionUID = 4309732999277305080L;
  private int numCore = -1;
  private boolean initialized = false;
  private LinkedList<Tuple> tupleQueue;

  public void test() {
    System.out.println("versiontest");
  }

  @Override
  public void cleanup() {
  }

  @Override
  public final void execute(Tuple input) {
    if (initialized == false){
      initalizeBolt();
      initialized = true;
    }
    executeLogic(input);
  }

  public final void execute(){
    while (!tupleQueue.isEmpty()){
      executeLogic(tupleQueue.poll());
    }
  }

  public void executeLogic(Tuple input){}

  public int getNumCore() {
    return numCore;
  }

  public void setNumCore(int numCore){
    this.numCore = numCore;
  }

  public void loadTuples(Tuple t){
    if (tupleQueue == null){
      tupleQueue = new LinkedList<>();
    }
    try {
      System.out.println(tupleQueue.size());
      tupleQueue.add(t);
    } catch (Exception e) {
      System.out.println("ErrorLoadingElasticTuples");
    }

  }

  public void initalizeBolt(){
    System.out.println("initializedddds");
  }



}