# Gary Data Engineer Homework

# Notes
1. is the video fast forwarded? can be answered by the data I think
2. run__uuid any operation on the same part gurantee to have same uuid? system restart?
3. Where is the 0,0,0 coordinate?
4. Use airflow each stage is a task that can be run independently
5. how to calculate total force velocity and acceleration
6. how to calculate, represent total distance traveled
7. what are the potential errors that you guys commonly see so I can incorporate into the pipeline.
8. ask for github handles to share repository with also download zip
9. docker + airflow


2.1 Preprocess and Clean
1. Read parquet
2. handling NaN values, expected values, outliers
3. datatypes, filling missing data, etc so that your pipeline doesn't break
4.  If one of the sensors was missing some data, would your pipeline fail? Does your pipeline assume a certain structure for the data? How does it accomodate changes or mistakes down stream from where your ETL pipeline runs? 
5. Use your best judgement given what you understand about the type of data given what cleansing steps make sense.


2.2 Convert timeseries to wide format
1. to making plots, training ML models, etc.
2. With your newly processed data, convert the timeseries data into a wide format that has the encoder values (x,y,z) and forces (fx,fy,fz) for each of the two robots (1,2) as individual columns. So rather than each row showing a time, sensor_type, robot_id, etc the data should show measurements for robots 1 & 2 corresponding to each encoder (e.g. x_1, y_1, z_1, x_2, y_2, z_2) and forces (fx_1, fy_1, fz_1, fx_2, fy_2, fz_2) for every timestamp.
3. you should transform the data to guarantee that any timestamp we access has a value for each column. Regardless which strategy you choose, explain why you chose it and what benefits or trade-offs it involves.
4. This data should also be saved in a way that these tables can be accessed by reference to the run_uuid.


2.3 Include Engineered/Calculated Features
1. You should include the follwing features for both robots (1 & 2)
2. 6 Velocity values (vx_1, vy_1, vz_1, vx_2, vy_2, vz_2)
3. 6 Acceleration values (ax_1, ay_1, az_1, ax_2, ay_2, az_2)
4. Total Velocity (v1, v2)
5. Total Acceleration (a1, a2)
6. Total Force (f1, f2)


2.4 Calculate Runtime Statistics
1. Produce a dataset that provides useful statistics about the different run_uuid information
2. run_uuid
3. run start time
4. run stop time
5. total runtime
6. total distance traveled


2.5 Store and Provide Access Tools
1. In the end, all of this information should be saved somewhere that is easy for other people to access. You can decide how you prefer to store this information. Maybe you prefer a SQL database. Maybe you prefer a tabular format. Choose the approach that you think makes the most sense for this specific setup. 
2. You can also choose how you want this information distributed. 
3. Do you think it should all be saved in one location?
4. Should it be saved across multiple separate data stores?


Tips
Try to follow ETL best practices for your example code. Good habits that we like to see might include:
- Making your tasks modular and easy to iterate on or modify. 
- Adding a new task into the pipeline shouldn't break the rest of the ETL flow.
- Enforce datatypes
- include exception handling
- include data cleaning measures for dealing with errors.
- Consider a design for your workflow that would make it easy to modify or update data as new features get added. 
- If you put all of the data in one location, how easy would it be to udpate or modify.
- If you spread your data across multiple locations, how would updates or modifications propagate to all those locations? 
- Consider processing speed and use parallel processing for independent tasks when possible. 
- Which parts of your pipeline can be parallelized? Which have to be done sequentially?
- Save data in formats that are easily extensible and convneint to query.
- Consider how your solution scales. If your implementation doesn't scale well, which parts would need the most/least modifcation? 
- Also, we strongly encourage you to include comments and descriptions of your thought process in your final solution.
- If you would like to submit a readme or document with notes and explanations for your ETL we will gladly take that into account when reviewing your code.
- Extra points for good Git hygiene.
- Send us the link to your repository.
- For example, if you choose to use python + pandas for data processing please include a requirements.txt file (in the case of pip) or environment.yml file (in the case of conda) as well as instructions for how we should run your solution code.


Assumptions
1. Note that this is not a production setup, main use case is to demonstrate the features and functionality of what a production system could loook like. Contains configuration and setup that are not best practices in production.
2. Tried to keep things simple


Requirement
1. Install Docker https://docs.docker.com/engine/install/, tested with community edition with 8G memory Docker Desktop 4.33.0, DockerCompose v2.14.0 or higher on MacOS

Commands
0. python3 -m venv .venv
0. source .venv/bin/activate
0. pip3 install -r dev-requirements.txt
0. cd airflow
1. make build
2. make run
3. docker ps -a
4. source .venv/bin/activate
5. 