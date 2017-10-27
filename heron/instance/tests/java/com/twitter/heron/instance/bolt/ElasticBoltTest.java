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
package com.twitter.heron.instance.bolt;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.twitter.heron.api.bolt.BaseElasticBolt;
import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.utils.Utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by zhengyang on 27/10/17.
 */
public class ElasticBoltTest {
  @Test
  public void ElasticBoltTest() {
    TestElasticBolt elasticBolt = getElasticBolt();

    testScale(elasticBolt);
    testLoadingTuples(elasticBolt);
  }

  public void testLoadingTuples(TestElasticBolt elasticBolt) {

    int seedNumTuples = 200;
    int numCore = 5;
    elasticBolt.setNumCore(5);
    assertEquals(numCore, elasticBolt.getNumCore());

    elasticBolt.initElasticBolt(getDummyCollector());

    // test to see if the dummy tuples are working
    assertEquals(getDummyTuple3().getString(0), "DummyString3");

    // load tuples
    for (int i = 0; i < seedNumTuples; i++) {
      elasticBolt.loadTuples(getDummyTuple1());
      if (i % 2 == 0) {
        elasticBolt.loadTuples(getDummyTuple2());
      }
    }
    elasticBolt.loadTuples(getDummyTuple3());

    // check inqueue size
    LinkedList<Tuple> inQueue = elasticBolt.getInqueue();
    assertEquals(seedNumTuples * 3 / 2 + 1, inQueue.size());

    // seedNumTuples * 1.5 , assuming even number + 1 type 3 tuple
    assertEquals(seedNumTuples * 3 / 2 + 1, elasticBolt.getOutstandingTuples());

    // 3 dummy tuple keys
    assertEquals(3, elasticBolt.getNumDistinctKeys());

    elasticBolt.testShard();

    // after sharding inqueue should be empty
    assertEquals(0, inQueue.size());

    // according to the order inserted, from rear end
    // tuples 1 is at thread 4, 2 at 3, 3 is at 2
    assertTrue(numCore - 1 == elasticBolt.getPKeyThreadMap().get("DummyString1"));
    assertTrue(numCore - 2 == elasticBolt.getPKeyThreadMap().get("DummyString2"));
    assertTrue(numCore - 3 == elasticBolt.getPKeyThreadMap().get("DummyString3"));

    // there should be seedNumTuples of tuples1
    assertEquals(seedNumTuples, elasticBolt.getQueue(numCore - 1).size());

    elasticBolt.runBolt();

    Utils.sleep(1000); // give it some time to process tuple

    // after processing there should be no more tuples left
    assertEquals(0, elasticBolt.getQueue(numCore - 1).size());
    assertEquals(0, elasticBolt.getOutstandingTuples());

  }

  public TestElasticBolt getElasticBolt() {
    return new TestElasticBolt();
  }

  public void testScale(TestElasticBolt elasticBolt) {
    // SCALING TEST
    elasticBolt.setNumCore(10);
    assertEquals(10, elasticBolt.getNumCore());
    elasticBolt.setMaxCore(20);
    assertEquals(20, elasticBolt.getMaxCore());


    // takes absolutes of input
    elasticBolt.scaleUp(-10);
    assertEquals(20, elasticBolt.getNumCore());

    // scaleUp should never let numCore execeed maxCore
    elasticBolt.scaleUp(1000);
    assertEquals(elasticBolt.getMaxCore(), elasticBolt.getNumCore());

    // scaleDown should never let numCore go below 1
    elasticBolt.scaleDown(40);
    assertEquals(1, elasticBolt.getNumCore());

    elasticBolt.scaleUp(4);
    assertEquals(5, elasticBolt.getNumCore());

  }

  class TestElasticBolt extends BaseElasticBolt {

    private static final long serialVersionUID = 4535804474224024218L;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map<String, Object> heronConf, TopologyContext context,
                        OutputCollector collector) {

    }

    @Override
    public void execute(Tuple tuple) {

    }

    public void testShard() {
      this.shardTuples();
    }

    public int getOutstandingTuples() {
      return this.getNumOutStanding();
    }

    public LinkedList<Tuple> getInqueue() {
      return this.inQueue;
    }

    public HashMap<String, Integer> getPKeyThreadMap() {
      return this.getKeyThreadMap();
    }
  }

  public OutputCollector getDummyCollector() {
    return new OutputCollector(new IOutputCollector() {
      @Override
      public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return null;
      }

      @Override
      public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors,
                             List<Object> tuple) {

      }

      @Override
      public void ack(Tuple input) {

      }

      @Override
      public void fail(Tuple input) {

      }

      @Override
      public void reportError(Throwable error) {

      }
    });
  }

  public Tuple getDummyTuple1() {
    return new Tuple() {
      @Override
      public int size() {
        return 0;
      }

      @Override
      public int fieldIndex(String field) {
        return 0;
      }

      @Override
      public boolean contains(String field) {
        return false;
      }

      @Override
      public Object getValue(int i) {
        return null;
      }

      @Override
      public String getString(int i) {
        return "DummyString1";
      }

      @Override
      public Integer getInteger(int i) {
        return null;
      }

      @Override
      public Long getLong(int i) {
        return null;
      }

      @Override
      public Boolean getBoolean(int i) {
        return null;
      }

      @Override
      public Short getShort(int i) {
        return null;
      }

      @Override
      public Byte getByte(int i) {
        return null;
      }

      @Override
      public Double getDouble(int i) {
        return null;
      }

      @Override
      public Float getFloat(int i) {
        return null;
      }

      @Override
      public byte[] getBinary(int i) {
        return new byte[0];
      }

      @Override
      public Object getValueByField(String field) {
        return null;
      }

      @Override
      public String getStringByField(String field) {
        return null;
      }

      @Override
      public Integer getIntegerByField(String field) {
        return null;
      }

      @Override
      public Long getLongByField(String field) {
        return null;
      }

      @Override
      public Boolean getBooleanByField(String field) {
        return null;
      }

      @Override
      public Short getShortByField(String field) {
        return null;
      }

      @Override
      public Byte getByteByField(String field) {
        return null;
      }

      @Override
      public Double getDoubleByField(String field) {
        return null;
      }

      @Override
      public Float getFloatByField(String field) {
        return null;
      }

      @Override
      public byte[] getBinaryByField(String field) {
        return new byte[0];
      }

      @Override
      public List<Object> getValues() {
        return null;
      }

      @Override
      public Fields getFields() {
        return null;
      }

      @Override
      public List<Object> select(Fields selector) {
        return null;
      }

      @Override
      public String getSourceComponent() {
        return null;
      }

      @Override
      public int getSourceTask() {
        return 0;
      }

      @Override
      public String getSourceStreamId() {
        return null;
      }

      @Override
      public void resetValues() {

      }
    };
  }

  public Tuple getDummyTuple2() {
    return new Tuple() {
      @Override
      public int size() {
        return 0;
      }

      @Override
      public int fieldIndex(String field) {
        return 0;
      }

      @Override
      public boolean contains(String field) {
        return false;
      }

      @Override
      public Object getValue(int i) {
        return null;
      }

      @Override
      public String getString(int i) {
        return "DummyString2";
      }

      @Override
      public Integer getInteger(int i) {
        return null;
      }

      @Override
      public Long getLong(int i) {
        return null;
      }

      @Override
      public Boolean getBoolean(int i) {
        return null;
      }

      @Override
      public Short getShort(int i) {
        return null;
      }

      @Override
      public Byte getByte(int i) {
        return null;
      }

      @Override
      public Double getDouble(int i) {
        return null;
      }

      @Override
      public Float getFloat(int i) {
        return null;
      }

      @Override
      public byte[] getBinary(int i) {
        return new byte[0];
      }

      @Override
      public Object getValueByField(String field) {
        return null;
      }

      @Override
      public String getStringByField(String field) {
        return null;
      }

      @Override
      public Integer getIntegerByField(String field) {
        return null;
      }

      @Override
      public Long getLongByField(String field) {
        return null;
      }

      @Override
      public Boolean getBooleanByField(String field) {
        return null;
      }

      @Override
      public Short getShortByField(String field) {
        return null;
      }

      @Override
      public Byte getByteByField(String field) {
        return null;
      }

      @Override
      public Double getDoubleByField(String field) {
        return null;
      }

      @Override
      public Float getFloatByField(String field) {
        return null;
      }

      @Override
      public byte[] getBinaryByField(String field) {
        return new byte[0];
      }

      @Override
      public List<Object> getValues() {
        return null;
      }

      @Override
      public Fields getFields() {
        return null;
      }

      @Override
      public List<Object> select(Fields selector) {
        return null;
      }

      @Override
      public String getSourceComponent() {
        return null;
      }

      @Override
      public int getSourceTask() {
        return 0;
      }

      @Override
      public String getSourceStreamId() {
        return null;
      }

      @Override
      public void resetValues() {

      }
    };
  }

  public Tuple getDummyTuple3() {
    return new Tuple() {
      @Override
      public int size() {
        return 0;
      }

      @Override
      public int fieldIndex(String field) {
        return 0;
      }

      @Override
      public boolean contains(String field) {
        return false;
      }

      @Override
      public Object getValue(int i) {
        return null;
      }

      @Override
      public String getString(int i) {
        return "DummyString3";
      }

      @Override
      public Integer getInteger(int i) {
        return null;
      }

      @Override
      public Long getLong(int i) {
        return null;
      }

      @Override
      public Boolean getBoolean(int i) {
        return null;
      }

      @Override
      public Short getShort(int i) {
        return null;
      }

      @Override
      public Byte getByte(int i) {
        return null;
      }

      @Override
      public Double getDouble(int i) {
        return null;
      }

      @Override
      public Float getFloat(int i) {
        return null;
      }

      @Override
      public byte[] getBinary(int i) {
        return new byte[0];
      }

      @Override
      public Object getValueByField(String field) {
        return null;
      }

      @Override
      public String getStringByField(String field) {
        return null;
      }

      @Override
      public Integer getIntegerByField(String field) {
        return null;
      }

      @Override
      public Long getLongByField(String field) {
        return null;
      }

      @Override
      public Boolean getBooleanByField(String field) {
        return null;
      }

      @Override
      public Short getShortByField(String field) {
        return null;
      }

      @Override
      public Byte getByteByField(String field) {
        return null;
      }

      @Override
      public Double getDoubleByField(String field) {
        return null;
      }

      @Override
      public Float getFloatByField(String field) {
        return null;
      }

      @Override
      public byte[] getBinaryByField(String field) {
        return new byte[0];
      }

      @Override
      public List<Object> getValues() {
        return null;
      }

      @Override
      public Fields getFields() {
        return null;
      }

      @Override
      public List<Object> select(Fields selector) {
        return null;
      }

      @Override
      public String getSourceComponent() {
        return null;
      }

      @Override
      public int getSourceTask() {
        return 0;
      }

      @Override
      public String getSourceStreamId() {
        return null;
      }

      @Override
      public void resetValues() {

      }
    };
  }

}
