/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backtype.storm.topology;

// TODO:- Add this
// import backtype.storm.grouping.CustomStreamGrouping;

import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.bolt.IElasticBolt;

import backtype.storm.generated.StormTopology;

public class TopologyBuilder {
  private com.twitter.heron.api.topology.TopologyBuilder delegate =
      new com.twitter.heron.api.topology.TopologyBuilder();

  public StormTopology createTopology() {
    HeronTopology topology = delegate.createTopology();
    return new StormTopology(topology);
  }

  public BoltDeclarer setBolt(String id, IRichBolt bolt) {
    return setBolt(id, bolt, null);
  }

  public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
    IRichBoltDelegate boltImpl = new IRichBoltDelegate(bolt);
    com.twitter.heron.api.topology.BoltDeclarer declarer =
        delegate.setBolt(id, boltImpl, parallelismHint);
    return new BoltDeclarerImpl(declarer);
  }

  public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
    return setBolt(id, bolt, null);
  }

  public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelismHint) {
    return setBolt(id, new BasicBoltExecutor(bolt), parallelismHint);
  }

  public BoltDeclarer setBolt(String id, IElasticBolt bolt, Number parallelismHint, Number numThread) {
    return setBolt(id, bolt, parallelismHint, numThread);
  }

  public SpoutDeclarer setSpout(String id, IRichSpout spout) {
    return setSpout(id, spout, null);
  }

  public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint) {
    IRichSpoutDelegate spoutImpl = new IRichSpoutDelegate(spout);
    com.twitter.heron.api.topology.SpoutDeclarer declarer =
        delegate.setSpout(id, spoutImpl, parallelismHint);
    return new SpoutDeclarerImpl(declarer);
  }
}
