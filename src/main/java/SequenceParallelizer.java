//import org.redisson.api.RAtomicLong;
//import org.redisson.api.RScoredSortedSet;
//import org.redisson.api.RSortedSet;
//import org.redisson.api.RedissonClient;
//import org.redisson.client.codec.LongCodec;
//import org.redisson.client.protocol.ScoredEntry;
//
//import java.util.*;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.function.Consumer;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//
//public class SequenceParallelizer<T> {
//
//    private RedissonClient redisClient;
//    private Function<T, String> keyExtractor;
//    private Consumer<Data<T>> messageHandler;
//
//    private Map<String, Map<Integer, AtomicLong>> partitionWatermarks = new ConcurrentHashMap<>();
//    private Map<String, Map<Integer, Map<String, Queue<Data<T>>>>> parallelismQueues = new ConcurrentHashMap<>();
//    private Map<String, Map<Integer, Set<Long>>> offsetCache = new ConcurrentHashMap<>();
//
//    private ExecutorService executorService = Executors.newCachedThreadPool();
//
//    public SequenceParallelizer(RedissonClient redisClient, Function<T, String> keyExtractor, Consumer<Data<T>> messageHandler) {
//        this.redisClient = redisClient;
//        this.keyExtractor = keyExtractor;
//        this.messageHandler = messageHandler;
//    }
//
//    private Queue<Data<T>> getTopicPartitionQueue(String topic, int partition, String key) {
//        return parallelismQueues
//                .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
//                .computeIfAbsent(partition, k -> new ConcurrentHashMap<>())
//                .computeIfAbsent(key, k -> initMessageQueue(topic, partition));
//    }
//
//    private AtomicLong getTopicPartitionWatermark(String topic, int partition) {
//        return partitionWatermarks
//                .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
//                .computeIfAbsent(partition, k -> initWatermark(topic, partition));
//    }
//
//    private AtomicLong initWatermark(String topic, int partition) {
//        RAtomicLong atomicLong = redisClient.getAtomicLong("parallel-partition-processor:" + topic + ":" + partition + "-watermark");
//        return new AtomicLong(atomicLong.get());
//    }
//
//    private LinkedBlockingQueue<Data<T>> initMessageQueue(String topic, int partition) {
//
//        RScoredSortedSet<Object> scoredSortedSet = redisClient.getScoredSortedSet("parallel-partition-processor:" + topic + ":" + partition+"-offsets", LongCodec.INSTANCE);
//        Collection<ScoredEntry<Object>> scoredEntries = scoredSortedSet.entryRange(Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, true);
//        List<Long> savedOffsets = scoredEntries.stream()
//                .map(e -> ((Long) e.getValue()))
//                .collect(Collectors.toList());
//        Set<Long> offsetCache = getOffsetCache(topic, partition);
//        offsetCache.clear();
//        offsetCache.addAll(savedOffsets);
//
//        LinkedBlockingQueue<Data<T>> messages = new LinkedBlockingQueue<>(1024);
//        startMessageHandler(messages, topic, partition);
//        return messages;
//    }
//
//    private void startMessageHandler(LinkedBlockingQueue<Data<T>> messages, String topic, int partition) {
//        executorService.submit(() -> {
//            Thread.currentThread().setName("parallel-partition-processor-"+topic+"-"+partition);
//            while (!Thread.interrupted()) {
//                try {
//                    Data<T> data = messages.take();
//                    boolean alreadyProcessed = isAlreadyProcessed(data);
//                    if(!alreadyProcessed) {
//                        messageHandler.accept(data);
//                        markProcessed(data);
//                    }
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                }
//            }
//        });
//    }
//
//    private Set<Long> getOffsetCache(String topic, int partition) {
//        return offsetCache
//                .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
//                .computeIfAbsent(partition, k -> new TreeSet<>());
//    }
//
//    private boolean isAlreadyProcessed(Data<T> data) {
//        return getOffsetCache(data.topic, data.partition).contains(data.offset);
//    }
//
//    private void markProcessed(Data<T> message) {
//        RScoredSortedSet<Object> scoredSortedSet = redisClient.getScoredSortedSet("parallel-partition-processor:" + message.topic + ":" + message.partition+"-offsets", LongCodec.INSTANCE);
//        scoredSortedSet.add(message.offset, message.offset);
//        getOffsetCache(message.topic, message.partition).add(message.offset);
//    }
//
//    private Long tryCommit(String topic, int partition) {
//        RScoredSortedSet<Long> scoredSortedSet = redisClient.getScoredSortedSet("parallel-partition-processor:" + topic + ":" + partition+"-offsets", LongCodec.INSTANCE);
//        long toCommit = getTopicPartitionWatermark(topic,partition).get();
//        Long countForCommit = drainSequence(scoredSortedSet, toCommit);
//        if (countForCommit == 0) {
//            return null;
//        }
//        toCommit += countForCommit;
//        RAtomicLong watermark = redisClient.getAtomicLong("parallel-partition-processor:" + topic + ":" + partition + "-watermark");
//        watermark.set(toCommit);
//        scoredSortedSet.removeRangeByScore(-1, true, toCommit, true);
//        System.out.println("Comitted: " + countForCommit);
//        return toCommit;
//    }
//
//    private static Long drainSequence(RScoredSortedSet<Long> scoredSortedSet, Long startFrom) {
//        boolean draining = true;
//        long oldOffset = startFrom;
//        long count = 0;
//        int range = 0;
//        while (draining) {
//            Collection<Long> offsets = scoredSortedSet.valueRange(range, true, 15000+range, false);
//            for (Long offset : offsets) {
//                if (startFrom >= offset) {
//                    continue;
//                }
//                if (oldOffset + 1 == offset) {
//                    oldOffset++;
//                    count++;
//                } else {
//                    draining = false;
//                    break;
//                }
//            }
//            range += 15000;
//        }
//        return count;
//    }
//
//    public void process(T message, String topic, int partition) {
//        String key = keyExtractor.apply(message);
//        getTopicPartitionQueue();
//    }
//}
