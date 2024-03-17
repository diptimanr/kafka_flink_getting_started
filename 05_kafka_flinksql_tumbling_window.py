
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

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
    #sensor_readings_tab = tenv.from_path('sensor_readings')

    # Process a Tumbling Window Aggregate Calculation of Ampere-Hour
    # For every 30 seconds non-overlapping window
    # Calculate the total charge consumed grouped by device
    tumbling_w_sql = """
            SELECT
                device_id,
                TUMBLE_START(proctime, INTERVAL '30' SECONDS) AS window_start,
                TUMBLE_END(proctime, INTERVAL '30' SECONDS) AS window_end,
                SUM(ampere_hour) AS charge_consumed
            FROM sensor_readings
            GROUP BY
                TUMBLE(proctime, INTERVAL '30' SECONDS),
                device_id
        """

    tumbling_w = tenv.sql_query(tumbling_w_sql)

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

