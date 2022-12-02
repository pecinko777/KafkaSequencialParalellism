import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.OverflowStrategy;
import akka.stream.RestartSettings;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.*;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.ScoredEntry;
import org.redisson.config.Config;
import scala.PartialFunction;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Main {

    static Map<String, Map<Integer, AtomicLong>> partitionWatermarks = new ConcurrentHashMap<>();
    static Map<String, Map<Integer, Set<Long>>> offsetCache = new ConcurrentHashMap<>();

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
                .setConnectionPoolSize(10)
                .setConnectionMinimumIdleSize(10)
                .setAddress("redis://127.0.0.1:6379");//
        RedissonClient r1 = Redisson.create(config);
        RedissonClient r2 = Redisson.create(config);
        RedissonClient r3 = Redisson.create(config);
        r1.getKeys().flushdb();
        RScoredSortedSet<Long> scoredSortedSet1 = r1.getScoredSortedSet("sortedCommits", LongCodec.INSTANCE);
        RScoredSortedSet<Long> scoredSortedSet2 = r2.getScoredSortedSet("sortedCommits", LongCodec.INSTANCE);
        RScoredSortedSet<Long> scoredSortedSet3 = r3.getScoredSortedSet("sortedCommits", LongCodec.INSTANCE);

        int pairs = 100;

        ExecutorService executorService1 = Executors.newFixedThreadPool(pairs);
        ExecutorService executorService2 = Executors.newFixedThreadPool(pairs);
        ExecutorService executorService3 = Executors.newFixedThreadPool(pairs);

        ActorSystem actorSystem = ActorSystem.create();
        RestartSettings restartSettings = RestartSettings.create(Duration.ofSeconds(1), Duration.ofSeconds(1), 1);


        Source<Data, NotUsed> source = Source.range(0, 200000000)
                .map(c -> new Data("Name", 0, c, c));

        Flow<Data, Filterable<Data>, NotUsed> flow = Flow.<Data>create()
                .groupBy(Integer.MAX_VALUE, e -> ((int) e.data) % 100)
                .buffer(1024, OverflowStrategy.backpressure())
                .mapAsync(1, d -> CompletableFuture.supplyAsync(() -> {
                            Optional<Long> lastWatermark = getLastWatermark(r1, d.topic, d.partition);
                            if (lastWatermark.orElse(-1L) >= d.offset) {
                                dupplicate.mark();
                                return new Filterable<>(d, false);
                            }
                            if (getCachedOffsets(r1, d.topic, d.partition).contains(d.offset)) {
                                dupplicate.mark();
                                return new Filterable<>(d, false);
                            }
                            return new Filterable<>(d, true);
                        })
                )
                .filter(d -> d.shouldProcess)
                .buffer(1024, OverflowStrategy.backpressure())
                .mapAsync(1, f -> CompletableFuture.supplyAsync(() -> {
                            insert.mark();
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            addOffset(r2, f.data.topic, f.data.partition, f.data.offset);
                            return f;
                        })
                )
                .mergeSubstreams()
                .buffer(1024, OverflowStrategy.backpressure())
                .filter(e -> e.data.offset % 10000 == 0)
                .mapAsync(1, l -> CompletableFuture.supplyAsync(() -> {
                            Optional<Long> lastCommit = getLastWatermark(r3, l.data.topic, l.data.partition);
                            long countForCommit = drainSequence(getCachedOffsets(r3, l.data.topic, l.data.partition), lastCommit.orElse(-1L));
                            long toCommit = lastCommit.orElse(-1L) + countForCommit;
                            if (countForCommit == 0) {
                                System.out.println("Skipped");
                                return l;
                            }
                            setLastWatermark(r3, l.data.topic, l.data.partition, toCommit);
                            removeRange(r3, l.data.topic, l.data.partition, lastCommit.orElse(-1L), toCommit);
                            System.out.println("Comitted: " + countForCommit);
                            return l;
                        })
                )
                .recoverWithRetries(Integer.MAX_VALUE, PartialFunction.empty());

        Source<Filterable<Data>, NotUsed> restartableSource = RestartSource.onFailuresWithBackoff(restartSettings, () -> source.via(flow));

        RunnableGraph<NotUsed> graph =
                restartableSource
                        .toMat(Sink.ignore(), Keep.left());

        graph.run(actorSystem);
    }

    private static void addOffset(RedissonClient redis, String topic, int partition, long offset) {
        Set<Long> cachedOffsets = getCachedOffsets(redis, topic, partition);
        RScoredSortedSet<Object> scoredSortedSet = redis.getScoredSortedSet("parallel-partition-processor:" + topic + ":" + partition + "-offsets", LongCodec.INSTANCE);
        scoredSortedSet.add(offset, offset);
        cachedOffsets.add(offset);
    }

    private static void removeRange(RedissonClient redis, String topic, int partition, long from, long to) {
        Set<Long> cachedOffsets = getCachedOffsets(redis, topic, partition);
        RScoredSortedSet<Object> scoredSortedSet = redis.getScoredSortedSet("parallel-partition-processor:" + topic + ":" + partition + "-offsets", LongCodec.INSTANCE);
        scoredSortedSet.removeRangeByScore(from, true, to, true);
        for (long i = from; i <= to; i++) {
            cachedOffsets.remove(i);
        }
    }

    private static Optional<Long> getLastWatermark(RedissonClient redis, String topic, int partition) {
        AtomicLong topicPartitionWatermark = getCachedWatermark(topic, partition);
        if (topicPartitionWatermark.get() == -1) {
            RAtomicLong atomicLong = redis.getAtomicLong("parallel-partition-processor:" + "topicName" + ":" + 0 + "-watermark");
            if(!redis.getBucket("parallel-partition-processor:" + "topicName" + ":" + 0 + "-watermark").isExists()) {
                atomicLong.set(-1);
            }
            topicPartitionWatermark.set(atomicLong.get());
            return Optional.empty();
        }
        return Optional.of(topicPartitionWatermark.get());
    }

    private static void setLastWatermark(RedissonClient redis, String topic, int partition, long offset) {
        redis.getAtomicLong("parallel-partition-processor:" + "topicName" + ":" + 0 + "-watermark").set(offset);
        getCachedWatermark(topic, partition).set(offset);
    }

    private static AtomicLong getCachedWatermark(String topic, int partition) {
        return partitionWatermarks
                .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(partition, k -> new AtomicLong(-1));
    }

    private static Set<Long> getCachedOffsets(RedissonClient redis, String topic, int partition) {
        return offsetCache
                .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(partition, k -> {
                    ConcurrentHashMap.KeySetView<Long, Boolean> cachedOffsets = ConcurrentHashMap.newKeySet();
                    RScoredSortedSet<Object> scoredSortedSet = redis.getScoredSortedSet("parallel-partition-processor:" + topic + ":" + partition + "-offsets", LongCodec.INSTANCE);
                    Collection<ScoredEntry<Object>> scoredEntries = scoredSortedSet.entryRange(0, Integer.MAX_VALUE);
                    Set<Long> storedOffsets = scoredEntries.stream().map(e -> ((Long) e.getValue())).collect(Collectors.toSet());
                    cachedOffsets.addAll(storedOffsets);
                    return cachedOffsets;
                });
    }

    private static Long drainSequence(Set<Long> offsets, Long lastRemoved) {
        long count = 0;
        long startFrom = lastRemoved == -1 ? 0 : lastRemoved;
        while (offsets.contains(startFrom + ++count)) {
        }
        return count-1;
    }

    static class Filterable<T> {
        T data;
        boolean shouldProcess;

        public Filterable(T data, boolean shouldProcess) {
            this.data = data;
            this.shouldProcess = shouldProcess;
        }

        @Override
        public String toString() {
            return "Filterable{" +
                    "data=" + data +
                    '}';
        }
    }

}
