build:
	docker build -t gary/ml_ops_data_engineer_hw:0.0.1 .

run:
	docker run --rm -d -p 8080:8080 -v ./dags/:/opt/airflow/dags/ -v ./data/:/opt/airflow/data/ --name="gary_ml_ops_data_engineer_hw" gary/ml_ops_data_engineer_hw:0.0.1

stop:
	docker stop gary_ml_ops_data_engineer_hw

clean:
	docker image rm gary/ml_ops_data_engineer_hw:0.0.1
	rm -r ./data/pipeline_artifacts

ssh:
	docker exec -it gary_ml_ops_data_engineer_hw /bin/bash