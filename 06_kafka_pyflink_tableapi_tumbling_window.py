import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble, Slide

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.in_streaming_mode()
    tenv = StreamTableEnvironment.create(env, settings)

    env.add_jars("file:///D:\\testing_space\\PycharmProjects\\kafka-flink-getting-started\\flink-sql-connector-kafka-3.1.0-1.18.jar")

    src_ddl = """
            CREATE TABLE sensor_readings (
                device_id VARCHAR,
                co DOUBLE,
                humidity DOUBLE,
                motion BOOLEAN,
                temp DOUBLE,
                ampere_hour DOUBLE,
                ts BIGINT,
                proctime AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'sensor.readings',
                'properties.bootstrap.servers' = 'localhost:9098',
                'properties.group.id' = 'device.tumbling.w.sql',
                'scan.startup.mode' = 'earliest-offset',
                'properties.auto.offset.reset' = 'earliest',
                'format' = 'json'
            )
        """

    tenv.execute_sql(src_ddl)
    sensor_readings_tab = tenv.from_path('sensor_readings')

    # Define a Tumbling Window Aggregate Calculation of ampere-hour sensor readings
    # - For every 30 seconds non-overlapping window
    # - Sum of charge consumed by each device
    tumbling_w = sensor_readings_tab.window(Tumble.over(lit(30).seconds)
                                            .on(sensor_readings_tab.proctime)
                                            .alias('w')) \
                .group_by(col('w'), sensor_readings_tab.device_id) \
                .select(sensor_readings_tab.device_id,
                    col('w').start.alias('window_start'),
                    col('w').end.alias('window_end'),
                    sensor_readings_tab.ampere_hour.sum.alias('charge_consumed'))


    sink_ddl = """
                CREATE TABLE devicecharge (
                    device_id VARCHAR,
                    window_start TIMESTAMP(3),
                    window_end TIMESTAMP(3),
                    charge_consumed DOUBLE
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = 'device.charge',
                    'properties.bootstrap.servers' = 'localhost:9098',
                    'scan.startup.mode' = 'earliest-offset',
                    'properties.auto.offset.reset' = 'earliest',
                    'format' = 'json'
                )
            """

    tenv.execute_sql(sink_ddl)
    tumbling_w.execute_insert('devicecharge').wait()

if __name__ == '__main__':
    main()