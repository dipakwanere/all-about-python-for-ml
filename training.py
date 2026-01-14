from datapipeline import DataProcessor

df = DataProcessor.read_csv_file("./lpdg_test.csv")
processor = DataProcessor(df)
final_df = (
    processor.clean()
    .add_features()
    .aggreagate_user_level()
    .get_data()
)