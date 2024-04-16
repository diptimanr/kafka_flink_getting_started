from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings, Schema, TableDescriptor
from pyflink.common import Row
from pyflink.table.udf import udf
import math

@udf(result_type=DataTypes.ROW([DataTypes.FIELD('device_id',DataTypes.STRING()),
                                    DataTypes.FIELD('total_charge_consumed', DataTypes.DOUBLE()),
                                    DataTypes.FIELD('avg_15', DataTypes.DOUBLE()),
                                    ]))
def device_stats_udf(device_history_readings: Row) -> Row:
    device_id, min15, min30, min45, min60 = device_history_readings
    quartile_readings = (min15, min30, min45, min60)
    total_charge_consumed = math.fsum(quartile_readings)
    avg_15 = total_charge_consumed / 4

    return Row(device_id, total_charge_consumed, avg_15)

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.in_batch_mode()
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=settings)

    field_names = ['device_id', 'min15', 'min30', 'min45', 'min60']
    field_types = [DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.DOUBLE(), DataTypes.DOUBLE(), DataTypes.DOUBLE()]
    schema = Schema.new_builder().from_fields(field_names, field_types).build()

    source_path_tableapi = 'sensor-history'
    tbl_env.create_table(
        'hourly_readings',
        TableDescriptor.for_connector('filesystem')
        .schema(schema)
        .option('path', f'{source_path_tableapi}')
        .format('csv')
        .build()
    )
    hourly_tab = tbl_env.from_path('hourly_readings')
    print('\n Hourly Readings Schema ::>')
    hourly_tab.print_schema()

    print('\n Hourly Readings Data ::>')
    hourly_tab.execute().print()

    device_stats = hourly_tab.map(device_stats_udf).alias('device_id', 'total_charge_consumed', 'avg_15')

    print('\n Device Stats Table Schema ::>')
    device_stats.print_schema()
    print('\n Device Stats Table Data ::>')

    sink_field_names = ['device_id', 'total_charge_consumed', 'avg_15']
    sink_field_types = [DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.DOUBLE()]
    sink_schema = Schema.new_builder().from_fields(sink_field_names, sink_field_types).build()

    source_path_tableapi = 'device_stats'
    tbl_env.create_table(
        'udf_sink',
        TableDescriptor.for_connector('filesystem')
        .schema(sink_schema)
        .option('path', f'{source_path_tableapi}')
        .format('csv')
        .build()
    )

    device_stats.execute_insert('udf_sink').print()

if __name__ == '__main__':
    main()





