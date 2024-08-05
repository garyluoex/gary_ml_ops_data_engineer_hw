select * from read_parquet('source_sensor_data/*/*.parquet');
select * from read_parquet('pipeline_artifacts/cleaning_pipeline/partition_sensor_data/*/*/*.parquet');
select * from read_parquet('pipeline_artifacts/cleaning_pipeline/clean_and_reshape/*/*.parquet');

create table sensor_data as
select * from read_parquet('source_sensor_data/*/*.parquet');

create table partitioned_sensor_data as
select * from read_parquet('pipeline_artifacts/cleaning_pipeline/partition_sensor_data/*/*/*.parquet');

create table pivoted_sensor_data as
select * from read_parquet('pipeline_artifacts/cleaning_pipeline/clean_and_reshape/*/*.parquet');

create table interpolate_sensor_data as
select * from read_parquet('pipeline_artifacts/cleaning_pipeline/interpolate_sensor_data/*/*.parquet');

SUMMARIZE sensor_data;
SUMMARIZE partitioned_sensor_data;
SUMMARIZE pivoted_sensor_data;
SUMMARIZE interpolate_sensor_data;


select run_uuid, field, count(*) from sensor_data group by run_uuid, field order by run_uuid, field;
select run_uuid, field, count(*) from partitioned_sensor_data group by run_uuid, field order by run_uuid, field;



select * from sensor_data left join partitioned_sensor_data 
on sensor_data.run_uuid = partitioned_sensor_data.run_uuid 
and sensor_data.time = partitioned_sensor_data.time 
and sensor_data.field = partitioned_sensor_data.field
and sensor_data.robot_id = partitioned_sensor_data.robot_id
where abs(sensor_data.value - partitioned_sensor_data.value) > 0.00000000001;