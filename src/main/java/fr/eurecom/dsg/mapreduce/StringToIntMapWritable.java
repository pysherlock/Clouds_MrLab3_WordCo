package fr.eurecom.dsg.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int 
 *
 **/
public class StringToIntMapWritable implements Writable {

  private Map<String, Integer> map;

  public StringToIntMapWritable () {
    map = new HashMap<>();
  }

  public void setStringToIntMapWritable(String word, int index) {
    map.put(word, index);
  }

  public void clean() {
    map.clear();
  }

  public Map<String, Integer> getMap() {
    return map;
  }

  public void add(StringToIntMapWritable stripe) {

    for(Map.Entry<String, Integer> entry : stripe.getMap().entrySet()) { //traverse the new stripe and add each entry the original one
      if(map.containsKey(entry.getKey())) {
        int value = map.get(entry.getKey()) + 1;
        map.put(entry.getKey(), value);
      }
      else {
        map.put(entry.getKey(), entry.getValue());
      }
    }
    //if(map.stripe.getMap())
  }


  // TODO: add an internal field that is the real associative array

  @Override
  public void readFields(DataInput in) throws IOException {

    // TODO: implement deserialization

    // Warning: for efficiency reasons, Hadoop attempts to re-use old instances of
    // StringToIntMapWritable when reading new records. Remember to initialize your variables 
    // inside this function, in order to get rid of old data.

  }

  @Override
  public void write(DataOutput out) throws IOException {

    // TODO: implement serialization
  }
}