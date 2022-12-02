class Data<T> {
    String topic;
    int partition;
    long offset;
    T data;

    public Data(String topic, int partition, long offset, T data) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
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
