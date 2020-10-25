package org.gwalid.seq.datasource.v2;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Scanner;

public class GenericArrayWritable extends ArrayWritable {
    private Writable[] values;
    public GenericArrayWritable() {
        super(Writable.class);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        values = new Writable[in.readInt()];          // construct values
        for (int i = 0; i < values.length; i++) {
            Class<? extends Writable> valueClass = resolveWritable(in);
            Writable value = WritableFactories.newInstance(valueClass);
            value.readFields(in);                       // read a value
            values[i] = value;                          // store it in values
        }
    }

    private Class<? extends Writable> resolveWritable(DataInput in){
//        in.readDouble()
//        Scanner sc = new Scanner(inStream);

        return LongWritable.class;
    }
}
