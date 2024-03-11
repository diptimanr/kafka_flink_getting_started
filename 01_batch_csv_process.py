from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes


def main():
    settings = EnvironmentSettings.in_batch_mode()
    tenv = TableEnvironment.create(settings)

    field_names = ['ts', 'device', 'co', 'humidity', 'light', 'lpg', 'motion', 'smoke', 'temp']
    field_types = [DataTypes.STRING(), DataTypes.STRING(),
                   DataTypes.STRING(), DataTypes.STRING(),
                   DataTypes.STRING(), DataTypes.STRING(),
                   DataTypes.STRING(), DataTypes.STRING(),
                   DataTypes.STRING()]
    schema = Schema.new_builder().from_fields(field_names, field_types).build()

    source_path_tableapi = 'sensor-source'
    tenv.create_table(
        'device_data',
        TableDescriptor.for_connector('filesystem')
        .schema(schema)
        .option('path', f'{source_path_tableapi}')
        .format('csv')
        .build()
    )

    device_tab =tenv.from_path('device_data')
    #print(device_tab.print_schema())
    #print(device_tab.to_pandas())

    distinct_devices = device_tab.select(device_tab.device).distinct()
    print(distinct_devices.to_pandas())


if __name__ == '__main__':
    main()

