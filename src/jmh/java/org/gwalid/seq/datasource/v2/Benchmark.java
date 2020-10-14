package org.gwalid.seq.datasource.v2;

import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class Benchmark {

    private SparkSession spark;

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(Benchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }


    @org.openjdk.jmh.annotations.Benchmark
    @Threads(1)
    public void countWithRDD() {
        new SeqDataSourceBenchmark(spark).countWithRDD();
    }

    @org.openjdk.jmh.annotations.Benchmark
    @Threads(1)
    public void countWithDS() {
        new SeqDataSourceBenchmark(spark).countWithDS();
    }


    @org.openjdk.jmh.annotations.Benchmark
    @Threads(1)
    public void aggQueryWithRDD() {
        new SeqDataSourceBenchmark(spark).aggQueryWithRDD();
    }

    @org.openjdk.jmh.annotations.Benchmark
    @Threads(1)
    public void aggQueryWithDS() {
        new SeqDataSourceBenchmark(spark).aggQueryWithDS();
    }

    @org.openjdk.jmh.annotations.Benchmark
    @Threads(1)
    public void filterQueryWithRDD() {
        new SeqDataSourceBenchmark(spark).filterQueryWithRDD();
    }

    @org.openjdk.jmh.annotations.Benchmark
    @Threads(1)
    public void filterQueryWithDS() {
        new SeqDataSourceBenchmark(spark).filterQueryWithDS();
    }


    @Setup
    public void setupBenchmark() {
        spark = SparkSession.builder().master("local[1]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        new SeqDataSourceBenchmark(spark).prepareDataset();
    }

    @TearDown
    public void tearDownBenchmark() {
        spark.close();
    }


}
