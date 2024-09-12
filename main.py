from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common.typeinfo import Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext, KeyedBroadcastProcessFunction
from pyflink.common import WatermarkStrategy, Encoder, Row
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee, KafkaOffsetResetStrategy
from pyflink.datastream.state import MapStateDescriptor
from dotenv import load_dotenv
import os
from pattern_broadcast_process import PatternBroadcastProcessFunction

load_dotenv()

kafka_host = os.getenv('KAFKA_HOST')

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
# env.enable_checkpointing(interval=3000)
env.set_python_executable("/home/mhtuan/anaconda3/envs/flink-env/bin/python")

user_action_type_info = Types.ROW_NAMED(["id", "action"], [Types.INT(), Types.STRING()])
user_pattern_type_info = Types.ROW_NAMED(["pattern"], [Types.OBJECT_ARRAY(Types.STRING())])
user_action_deserialization_schema = JsonRowDeserializationSchema.builder().type_info(
                             type_info=user_action_type_info).build()
user_pattern_deserialization_schema = JsonRowDeserializationSchema.builder().type_info(
                             type_info=user_pattern_type_info).build()

user_action_source = KafkaSource.builder() \
        .set_bootstrap_servers(f"{kafka_host}:9091,{kafka_host}:9092,{kafka_host}:9093") \
        .set_topics("user-action") \
        .set_group_id("flink-1") \
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
        .set_value_only_deserializer(user_action_deserialization_schema) \
        .build()

user_pattern_source = KafkaSource.builder() \
        .set_bootstrap_servers(f"{kafka_host}:9091,{kafka_host}:9092,{kafka_host}:9093") \
        .set_topics("user-pattern") \
        .set_group_id("flink-1") \
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
        .set_value_only_deserializer(user_pattern_deserialization_schema) \
        .build()

ds_user_action = env.from_source(user_action_source, WatermarkStrategy.no_watermarks(), "Action Source")
ds_pattern = env.from_source(user_pattern_source, WatermarkStrategy.no_watermarks(), "Pattern Source")

key_action_ds = ds_user_action.key_by(lambda record: record["id"], key_type=Types.INT())
pattern_state_desc = MapStateDescriptor(
    "pattern",
    Types.INT(),
    Types.OBJECT_ARRAY(Types.STRING())
)
pattern_broadcast_stream = ds_pattern.broadcast(pattern_state_desc)

pattern_matched_ds = key_action_ds.connect(pattern_broadcast_stream).process(PatternBroadcastProcessFunction())

pattern_matched_ds.print()

env.execute("kafka_fraud")