from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
import os
import glob
from pyflink.datastream.connectors.kafka import KafkaTopicPartition, KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from models import tripUpdatesModel
from pyflink.common import WatermarkStrategy, Row, Types
from datetime import datetime
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions

def setupStreamEnvironment():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    project_root = os.getcwd()
    parentDirectory = os.path.dirname(project_root)
    jarPath = os.path.join(parentDirectory, "jars/*")
    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")


    jars = tuple(
    [f"file://{os.path.join(parentDirectory, jar)}" for jar in glob.glob(jarPath)]
)

    env.add_jars(*jars)
    return env,BOOTSTRAP_SERVERS

def extract_trip_info(row):
    trip_id = row.id
    start_time = datetime.strptime(row.trip_update.trip.start_time, "%H:%M:%S")
    start_date = datetime.strptime(row.trip_update.trip.start_date, "%Y%m%d")
    for stops in row.trip_update.stop_time_update:
        stop_seq = stops.stop_sequence
        if stops.departure is not None:
            if stops.departure.delay > 0:
                delay_in_seconds = stops.departure.delay
                status = "delayed"
            elif stops.departure.delay < 0:
                delay_in_seconds = abs(stops.departure.delay)
                status = "early"
            else:
                delay_in_seconds = stops.departure.delay
                status = "running on time"
            stop_time = datetime.fromtimestamp(stops.departure.time)
        else:
            if stops.arrival.delay > 0:
                delay_in_seconds = stops.arrival.delay
                status = "delayed"
            elif stops.arrival.delay < 0:
                delay_in_seconds = abs(stops.arrival.delay)
                status = "early"
            else:
                delay_in_seconds = stops.arrival.delay
                status = "running on time"
            stop_time = datetime.fromtimestamp(stops.arrival.time)
        yield Row(trip_id, start_time, start_date, stop_seq, stop_time, delay_in_seconds, status)

row_type_info = Types.ROW([Types.STRING(), Types.SQL_TIME(), Types.SQL_DATE(), Types.INT(),Types.SQL_TIMESTAMP(), Types.INT(), Types.STRING()])

env, BOOTSTRAP_SERVERS = setupStreamEnvironment()

tripUpdatesPartition = {
    KafkaTopicPartition("transitStream", 0)
}

tripUpdatesSource = (
    KafkaSource.builder()
    .set_bootstrap_servers(BOOTSTRAP_SERVERS)
    .set_group_id("flink.testertransit")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(
        JsonRowDeserializationSchema.builder()
        .type_info(tripUpdatesModel.tripUpdates.tripUpdateRow())
        .build()
    )
    .set_partitions(tripUpdatesPartition)
    .build()
    )

tripUpdatesFlinkStream = env.from_source(
        tripUpdatesSource, WatermarkStrategy.no_watermarks(), "flink kafka trip update source"
    )

tripUpdatesFlinkStream.flat_map(extract_trip_info).add_sink(
    JdbcSink.sink(
        """INSERT INTO public.trip_updates (trip_id, start_time, start_date, stop_seq, stop_time, delay_in_sec, status)
            VALUES (?, ?, ?, ?, ?, ?, ?) 
            ON CONFLICT (trip_id, start_date, stop_seq) 
            DO UPDATE SET 
            trip_id = EXCLUDED.trip_id,
            start_time = EXCLUDED.start_time,
            start_date = EXCLUDED.start_date,
            stop_seq = EXCLUDED.stop_seq,
            stop_time = EXCLUDED.stop_time,
            delay_in_sec = EXCLUDED.delay_in_sec,
            status = EXCLUDED.status
        """,
        row_type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url('jdbc:postgresql://localhost:5432/transitstreamtest')
            .with_driver_name('org.postgresql.Driver')
            .with_user_name('root')
            .with_password('root')
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_interval_ms(1000)
            .with_batch_size(200)
            .with_max_retries(5)
            .build()
)
)

env.execute()
env.close()