package com.twitter.elephantbird.mapreduce.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


//TODO override EVERYTHING
public class EbCombineFileInputFormat<K, V> extends CombineFileInputFormat<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(EbCombineFileInputFormat.class);

    public static void setSplitMinSizePerNode(Configuration conf, long value) {
        conf.setLong(SPLIT_MINSIZE_PERNODE, value);
    }

    public static void setSplitMinSizePerRack(Configuration conf, long value) {
        conf.setLong(SPLIT_MINSIZE_PERRACK, value);
    }

    private InputFormat<K, V> delegate;
    private long maxSplitSize;
    private long minSplitSizeNode;
    private long minSplitSizeRack;

    // A pool of input paths filters. A split cannot have blocks from files
    // across multiple pools.
    private ArrayList<MultiPathFilter> pools = new  ArrayList<MultiPathFilter>();

    public EbCombineFileInputFormat(InputFormat<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public RecordReader createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        /*
        if (!(inputSplit instanceof CompositeInputSplit)) {
            throw new IOException("Cannot create record reader from anything but a CompositeInputSplit. " +
                    "Received: " + inputSplit);
        }
        */
        try {
            return delegate.createRecordReader(inputSplit, taskAttemptContext);
        } catch (InterruptedException e) {
            throw new IOException("Delegate error on createRecordReader");
        }
    }

    @Override
    protected void setMaxSplitSize(long maxSplitSize) {
        this.maxSplitSize = maxSplitSize;
    }

    @Override
    protected void setMinSplitSizeNode(long minSplitSizeNode) {
        this.minSplitSizeNode = minSplitSizeNode;
    }

    @Override
    protected void setMinSplitSizeRack(long minSplitSizeRack) {
        this.minSplitSizeRack = minSplitSizeRack;
    }

    @Override
    protected void createPool(List<PathFilter> filters) {
        pools.add(new MultiPathFilter(filters));
    }

    @Override
    protected void createPool(PathFilter... filters) {
        MultiPathFilter multi = new MultiPathFilter();
        for (PathFilter f: filters) {
            multi.add(f);
        }
        pools.add(multi);
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        //TODO for testing, try just combining into one. We will see if that works. So 4 => 1. Get this
        // plumbed through. We probably need a scheme enhancement to work with cascading?
        // Need to basically make sure that we can get it working in the dumbest case.
        // That is: reading 4 lzo files, say, on 1 mapper.
        CompositeInputSplit compositeInputSplit = new CompositeInputSplit();
        try {
            for (InputSplit split : delegate.getSplits(job)) {
                compositeInputSplit.add((FileSplit) split);
            }
        } catch (InterruptedException e) {
            throw new IOException("Delegate error on getSplits", e);
        }
        List<InputSplit> inputSplits = new ArrayList<InputSplit>(1);
        inputSplits.add(compositeInputSplit);
        return inputSplits;
    }

    /**
     * Accept a path only if any one of filters given in the
     * constructor do. This is taken from {@link CombineFileInputFormat}
     * as it does not make it public.
     */
    public static final class MultiPathFilter implements PathFilter {
        private List<PathFilter> filters;

        public MultiPathFilter() {
            this.filters = new ArrayList<PathFilter>();
        }

        public MultiPathFilter(List<PathFilter> filters) {
            this.filters = filters;
        }

        public void add(PathFilter one) {
            filters.add(one);
        }

        public boolean accept(Path path) {
            for (PathFilter filter : filters) {
                if (filter.accept(path)) {
                    return true;
                }
            }
            return false;
        }

        public String toString() {
            StringBuffer buf = new StringBuffer();
            buf.append("[");
            for (PathFilter f: filters) {
                buf.append(f);
                buf.append(",");
            }
            buf.append("]");
            return buf.toString();
        }
    }
}
