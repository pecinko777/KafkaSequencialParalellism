import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.OverflowStrategy;
import akka.stream.RestartSettings;
import akka.stream.javadsl.*;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.redisson.Redisson;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.LongCodec;
import org.redisson.config.Config;
import scala.PartialFunction;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    public static void main(String[] args) {
        MetricRegistry metricRegistry = new MetricRegistry();
        MyConsoleReporter reporter = MyConsoleReporter.forRegistry(metricRegistry).build();
        reporter.start(1, TimeUnit.SECONDS);

        Meter insert = metricRegistry.meter("Insert");
        Meter dupplicate = metricRegistry.meter("Duplicate");


//        metricRegistry.register("Sequence", new Gauge<Integer>() {
//            @Override
//            public Integer getValue() {
//                return processedOffsets.size();
//            }
//        });
        Config config = new Config();
        config.useSingleServer()
                .setConnectionPoolSize(8)
                .setConnectionMinimumIdleSize(8)
//                .setSubscriptionConnectionPoolSize(8)
//                .setSubscriptionConnectionMinimumIdleSize(8)
                // use "rediss://" for SSL connection
                .setAddress("redis://127.0.0.1:6379");//
        RedissonClient r1 = Redisson.create(config);
        RedissonClient r2 = Redisson.create(config);
        RedissonClient r3 = Redisson.create(config);
//        r.getKeys().flushdb();
        RScoredSortedSet<Long> scoredSortedSet1 = r1.getScoredSortedSet("sortedCommits", LongCodec.INSTANCE);
        RScoredSortedSet<Long> scoredSortedSet2 = r2.getScoredSortedSet("sortedCommits", LongCodec.INSTANCE);
        RScoredSortedSet<Long> scoredSortedSet3 = r3.getScoredSortedSet("sortedCommits", LongCodec.INSTANCE);

        int pairs = 100;

        ExecutorService executorService1 = Executors.newFixedThreadPool(pairs * 6);
        ExecutorService executorService2 = Executors.newFixedThreadPool(pairs * 2 * 50);
        ExecutorService executorService3 = Executors.newFixedThreadPool(16);

        ActorSystem actorSystem = ActorSystem.create();
        RestartSettings restartSettings = RestartSettings.create(Duration.ofSeconds(1), Duration.ofSeconds(1), 1);

        Long start = (Long) r3.getBucket("commited", LongCodec.INSTANCE).get();
        if(start == null) {
            start = 0L;
        }
        scoredSortedSet3.removeRangeByScore(-1, true, start, true);
        var lastCommit = start;
        AtomicLong watermark = new AtomicLong(lastCommit);
        Source<Data, NotUsed> source = Source.range(Math.toIntExact(start), 200000000)
                .map(c -> new Data(c % pairs, c, c));

        Flow<Data, Filterable<Data>, NotUsed> flow = Flow.<Data>create()
                .groupBy(Integer.MAX_VALUE, e -> e.partition)
                .buffer(128, OverflowStrategy.backpressure())
                .mapAsync(4, d -> CompletableFuture.supplyAsync(() -> {
                            if ((watermark.get() != -1 && watermark.get() >= d.offset)) {
                                dupplicate.mark();
                                scoredSortedSet1.add(d.offset, d.offset);
                                return new Filterable<>(d, false);
                            }
                            if (scoredSortedSet1.contains(d.offset)) {
                                dupplicate.mark();
                                scoredSortedSet1.add(d.offset, d.offset);
                                return new Filterable<>(d, false);
                            }
                            return new Filterable<>(d, true);
                        }, executorService1)
                )
                .filter(d -> d.shouldProcess)
                .buffer(128, OverflowStrategy.backpressure())
                .mapAsync(1, f -> CompletableFuture.supplyAsync(() -> {
                            try {
                                Thread.sleep(15);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            insert.mark();
                            scoredSortedSet2.add(f.data.offset, f.data.offset);
                            return f;
                        }, executorService2)
                )
//                .recoverWithRetries(Integer.MAX_VALUE, PartialFunction.empty())
                .mergeSubstreams()
                .filter(e -> e.data.offset % 10000 == 0)
                .buffer(128, OverflowStrategy.backpressure())
                .mapAsync(1, l -> CompletableFuture.supplyAsync(() -> {
                            Long toCommit = watermark.get();
                            Long countForCommit = drainSequence(scoredSortedSet3, toCommit);
                            toCommit += countForCommit;
                            if (toCommit == watermark.get()) {
                                System.out.println("Skipped");
                                return l;
                            }
//                            if (new Random().nextInt(100) > 80) throw new RuntimeException();
                            r3.getBucket("commited", LongCodec.INSTANCE).set(toCommit);
                            scoredSortedSet3.removeRangeByScore(-1, true, toCommit, true);
//                            r3.getTopic("q").publish(Map.of("from",watermark.get()+1, "to", toCommit));
                            watermark.set(toCommit);
                            System.out.println("Comitted: " + countForCommit);
                            return l;
                        }, executorService3)
                )
                .recoverWithRetries(Integer.MAX_VALUE, PartialFunction.empty());

        Source<Filterable<Data>, NotUsed> restartableSource = RestartSource.onFailuresWithBackoff(restartSettings, () -> source.via(flow));
        Flow<Data, Filterable<Data>, NotUsed> restartableFlow = RestartFlow.onFailuresWithBackoff(restartSettings, () -> flow);

        RunnableGraph<NotUsed> graph =
                restartableSource
                        .toMat(Sink.ignore(), Keep.left());

        graph.run(actorSystem);
    }

    private static Long drainSequence(RScoredSortedSet<Long> scoredSortedSet3, Long startFrom) {
        boolean draining = true;
        long oldOffset = startFrom;
        long count = 0;
        int range = 0;
        while (draining) {
            Collection<Long> offsets = scoredSortedSet3.valueRange(range, true, 15000+range, false);
            for (Long offset : offsets) {
                if (startFrom >= offset) {
                    continue;
                }
                if (oldOffset + 1 == offset) {
                    oldOffset++;
                    count++;
                } else {
                    draining = false;
                    break;
                }
            }
            range += 15000;
        }
        return count;
    }

    static class Filterable<T> {
        T data;
        boolean shouldProcess;

        public Filterable(T data, boolean shouldProcess) {
            this.data = data;
            this.shouldProcess = shouldProcess;
        }

        public T getData() {
            return data;
        }

        public void setData(T data) {
            this.data = data;
        }

        public boolean isShouldProcess() {
            return shouldProcess;
        }

        public void setShouldProcess(boolean shouldProcess) {
            this.shouldProcess = shouldProcess;
        }

        @Override
        public String toString() {
            return "Filterable{" +
                    "data=" + data +
                    '}';
        }
    }

    static class Data {
        int partition;
        long offset;
        long data;

        public Data(int partition, long offset, long data) {
            this.partition = partition;
            this.offset = offset;
            this.data = data;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public long getData() {
            return data;
        }

        public void setData(long data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return "Data{" +
                    "partition=" + partition +
                    ", offset=" + offset +
                    ", data=" + data +
                    '}';
        }
    }

}
