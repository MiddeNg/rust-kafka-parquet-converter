use std::fs::File;
use polars::prelude::*;
use polars::io::parquet::BatchedWriter;
use schema_registry_converter::blocking::avro::AvroDecoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::config::ClientConfig;
use rdkafka::TopicPartitionList;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use rdkafka::util::Timeout;

fn main() {
  let schema_registry_url = "http://localhost:8081".to_string();
  let topic = "as2.json.vts";
  let schema_registry_settings = SrSettings::new(schema_registry_url);
  let decoder = AvroDecoder::new(schema_registry_settings);
  let consumer: BaseConsumer = ClientConfig::new()
    .set("bootstrap.servers", "localhost:9092")
    .set("group.id", "rust-test-consumer")
    .set("enable.partition.eof", "false")
    .set("session.timeout.ms", "6000")
    // Commit automatically every 5 seconds.
    .set("enable.auto.commit", "true")
    .set("auto.commit.interval.ms", "5000")
    // but only commit the offsets explicitly stored via `consumer.store_offset`.
    .set("enable.auto.offset.store", "false")
    .create()
    .expect("Consumer creation failed");
  consumer.subscribe(&[topic]).expect("Can't subscribe to specified topic");
  let duration = Duration::from_secs(10);
  // let mut assignment = TopicPartitionList::new();
  // assignment.add_partition_offset(topic, 0, rdkafka::Offset::Beginning).unwrap();
  let timestamp_ms: i64 = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .expect("Failed to get system time")
    .as_millis() as i64;
  let result = consumer.offsets_for_timestamp(timestamp_ms, duration).unwrap();
  // let offsets = consumer.offsets_for_times(assignment, timeout).unwrap();
  println!("{:?}", result.to_topic_map());
  let dummydf = df!(
      "a" => &[1, 2, 3],
      "b" => &[4, 5, 6],
      "c" => &[7, 8, 9],
    ).unwrap();
  let mut batched_writer: BatchedWriter<File> = create_batched_writer(&dummydf.schema());
  {
    let df1 = df!(
      "a" => &[1, 2, 3],
      "b" => &[4, 5, 6],
      "c" => &[7, 8, 9],
    ).unwrap();
    let df2 = df!(
      "a" => &[10, 20, 30],
      "b" => &[40, 50, 60],
      "c" => &[70, 80, 90],
    ).unwrap();
    let df3 = df!(
      "a" => &[100, 200, 300],
      "b" => &[400, 500, 600],
      "c" => &[700, 800, 900],
    ).unwrap();
    let df_vec = vec![df1, df2, df3];

    for df in df_vec.iter() {
      batched_writer.write_batch(&df).unwrap();
    }
  }
  batched_writer.finish().unwrap();
  read_parquet_to_df()
}

pub fn create_batched_writer(schema: &polars::prelude::Schema) -> BatchedWriter<File> {
  let mut file = std::fs::File::create("output/test.parquet").unwrap();
  let writer = ParquetWriter::new(file);
  writer.batched(schema).unwrap()
}


fn read_parquet_to_df() {
  let mut file = std::fs::File::open("output/test.parquet").unwrap();

  let df = ParquetReader::new(&mut file).finish().unwrap();
  println!("{:?}", df);
}
