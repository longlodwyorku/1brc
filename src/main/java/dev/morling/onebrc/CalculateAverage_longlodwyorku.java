/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import static java.util.stream.Collectors.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import sun.misc.Unsafe;

public class CalculateAverage_longlodwyorku {

    private static final String FILE = "./measurements.txt";
    private static Unsafe unsafe;

    private static void initUnsafe() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        unsafe = (Unsafe) f.get(null);

    }

    private static class StationData {
        double sum = 0;
        long count = 0;
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        
        public StationData(float v) {
            sum = v;
            count = 1;
            min = v;
            max = v;
        }
    }

    private static class ByteView {
        long address;
        long size;

        public ByteView(long address, long size) {
            this.address = address;
            this.size = size;
        }

        @Override
        public int hashCode() {
            // TODO Auto-generated method stub
            return (int)address;
        }

        public float parse() {
            
        }
    }

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    private static void findDeliminators(long address, long end, ArrayList<Long> indexes) {
        for (long start = address; start < end; ++start) {
            byte cur = unsafe.getByte(start);
            if (cur == ';' | cur == '\n')
            {
                indexes.add(start);
            }
        }
    }

    private static void mergeDeliminators(ArrayList<Long> delims, int start, ArrayList<Long> source) {
        delims.addAll(start, source);
    }

    private static void parse(Map<ByteView, StationData> result, long address, ArrayList<Long> delims, int start_delim, int end_delim) {
        ByteView cur = new ByteView(0, 0);
        for (; start_delim < end_delim; start_delim += 2) {
            cur.address = address;
            cur.size = delims.get(start_delim);
            if (result.containsKey(cur)) {
                StationData data = result.get(cur);
                float v = Float.parseFloat(FILE)
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int cores = Runtime.getRuntime().availableProcessors();
        /*Thread[] threads = new Thread[cores];
        for (int k = cores; k != 0; --k) {
            threads[k] = new Thread();
        }*/

        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        FileChannel channel = file.getChannel();
//        MappedByteBuffer mb = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        long chunk_size = channel.size() / cores;

        channel.close();
        file.close();

        // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
        // .map(l -> l.split(";"))
        // .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));
        //
        // measurements1 = new TreeMap<>(measurements1.entrySet()
        // .stream()
        // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));
        // System.out.println(measurements1);

        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    a.min = Math.min(a.min, m.value);
                    a.max = Math.max(a.max, m.value);
                    a.sum += m.value;
                    a.count++;
                },
                (agg1, agg2) -> {
                    var res = new MeasurementAggregator();
                    res.min = Math.min(agg1.min, agg2.min);
                    res.max = Math.max(agg1.max, agg2.max);
                    res.sum = agg1.sum + agg2.sum;
                    res.count = agg1.count + agg2.count;

                    return res;
                },
                agg -> {
                    return new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
                });

        Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(FILE))
                .map(l -> new Measurement(l.split(";")))
                .collect(groupingBy(m -> m.station(), collector)));

        System.out.println(measurements);
    }
}
