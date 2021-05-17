package org.apache.hadoop.examples;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.util.Set;

class MyMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        Set<Writable> keySet = this.keySet();

        for (Object key : keySet) {
            result.append(key.toString() + " = " + this.get(key) + ",");
        }
        return result.toString();
    }
}
