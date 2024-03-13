from pyflink.table import (EnvironmentSettings, TableEnvironment,
                           TableDescriptor, Schema, DataTypes)


def main():
    settings = EnvironmentSettings.in_batch_mode()
    tenv = TableEnvironment.create(settings)

    # Using FlinkSQL
    # ================
    sensor_source_ddl = """
                CREATE TABLE device_data(
                    ts VARCHAR,
                    device VARCHAR,
                    co VARCHAR,
                    humidity VARCHAR,
                    light VARCHAR,
                    lpg VARCHAR,
                    motion VARCHAR,
                    smoke VARCHAR,
                    temp VARCHAR
                ) WITH (
                    'connector' = 'filesystem',
                    'path' = 'sensor-source',
                    'format' = 'csv'
                )
            """
    tenv.execute_sql(sensor_source_ddl)
    device_tab = tenv.from_path('device_data')
    print('\n Device Table Schema - using FinkSQL')
    device_tab.print_schema()
    print('\nSensor Data - using FlinkSQL')
    tenv.sql_query('SELECT * FROM device_data LIMIT 10').execute().print()
    print('\n')

    unique_devices_sql = """
            SELECT DISTINCT device
            FROM device_data
        """
    print('Unique Devices : \n')
    print(tenv.sql_query(unique_devices_sql).execute().print())

    high_temp_devices_sql = """
            SELECT ts,device,temp
            FROM device_data
            WHERE temp >= '20'
            ORDER BY temp DESC
            LIMIT 20
        """
    print('High Temp Devices : \n')
    print(tenv.sql_query(high_temp_devices_sql).execute().print())

    print("Explain plan for high_temp_device query - FlinkSQL\n")
    high_temp_device_sql_explanation = tenv.explain_sql(high_temp_devices_sql)
    print(high_temp_device_sql_explanation)

if __name__ == '__main__':
    main()
