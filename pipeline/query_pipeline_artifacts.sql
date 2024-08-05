create table source_data as select * from read_parquet('data/source/*/*.parquet');

create table paritioned_data as select * from read_parquet('data/pipeline_artifacts/preprocess_pipeline/partitioned_data/*/*/*.parquet');

create table pivoted_data as select * from read_parquet('data/pipeline_artifacts/preprocess_pipeline/pivoted_data/*/*.parquet');

create table interpolated_data as select * from read_parquet('data/pipeline_artifacts/preprocess_pipeline/interpolated_data/*/*.parquet');

create table combined_data as select * from read_parquet('data/pipeline_artifacts/features_pipeline/combined_data/*/*.parquet');

create table run_statistics as select * from read_parquet('data/pipeline_artifacts/summary_pipeline/statistics_data/*.parquet');

SUMMARIZE source_data;
SUMMARIZE paritioned_data;
SUMMARIZE pivoted_data;
SUMMARIZE combined_data;
SUMMARIZE run_statistics;