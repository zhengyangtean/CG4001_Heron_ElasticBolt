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
public class BaseKeyLoadTuple implements Comparable<BaseKeyLoadTuple> {
  private String k;
  private int v;

  public BaseKeyLoadTuple(String k, int v) {
    this.k = k;
    this.v = v;
  }

  public String getT() {
    return k;
  }

  public int getV() {
    return v;
  }

  @Override
  public int compareTo(BaseKeyLoadTuple t) {
    if (this.v < t.v) {
      return -1;
    }
    else if(this.v > t.v){
      return 1;
    }
    return 0;
  }
}
