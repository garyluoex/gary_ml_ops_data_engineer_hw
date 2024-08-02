# Data Engineer Homework

# Objective
The goal for this assignment is to design a simple ETL process that converts a sample of raw timeseries data into a collection of useful datasets and statistics relevant for work we do at Machina Labs. You can use whatever tools you like but make sure to include instructions in your submission for how your submission should be used and what dependencies it has. For example, if you choose to use python + pandas for data processing please include a `requirements.txt` file (in the case of pip) or `environment.yml` file (in the case of conda) as well as instructions for how we should run your solution code. 


# ETL Example
## 1. Context
At Machina Labs we use a pair of robotic arms to form sheet metal into custom geometries designed in CAD software. This is done by defining a parametric path and allowing the pair of robot arms to slowly pushing the metal sheet, layer by layer, into the desired shape. A demo video showing this process is given in this repository under `demo_video.mp4`

While the robots are running we record large quantities of data in a time-series format tracking the position of the robot arms and the amount of force the arms experience using a sensor called a ``load cell``. An example of this data is given in the file `data/sample.parquet` and is shown below:

```
|---------|----------------------------|-----------------------|---------|------------|------------------------|---------------|
|         | time                       | value                 | field   | robot_id   | run_uuid               | sensor_type   |
| ------- | -------------------------- | --------------------- | ------- | ---------- | ---------------------- | ------------- |
| 0       | 2022-11-23T20:40:00.444Z   | 826.1516              | x       | 1          | 8910095844186657261    | encoder       |
| ------- | -------------------------- | --------------------- | ------- | ---------- | ---------------------- | ------------- |
| 1       | 2022-11-23T20:40:02.111Z   | 882.4617              | x       | 1          | 8910095844186657261    | encoder       |
| ------- | -------------------------- | --------------------- | ------- | ---------- | ---------------------- | ------------- |
| 2       | 2022-11-23T20:40:03.778Z   | 846.7317              | x       | 1          | 8910095844186657261    | encoder       |
| ------- | -------------------------- | --------------------- | ------- | ---------- | ---------------------- | ------------- |

|---------|----------------------------|-----------------------|---------|------------|------------------------|---------------|
| 8664    | 2022-11-23T20:40:00.444Z   | -1132.949052734375    | fx      | 1          | 7582293080991470061    | load_cell     |
| ------- | -------------------------- | --------------------- | ------- | ---------- | ---------------------- | ------------- |
| 8665    | 2022-11-23T20:40:02.111Z   | -906.7292041015623    | fx      | 1          | 7582293080991470061    | load_cell     |
| ------- | -------------------------- | --------------------- | ------- | ---------- | ---------------------- | ------------- |
| 8666    | 2022-11-23T20:40:03.778Z   | -307.31151000976564   | fx      | 1          | 7582293080991470061    | load_cell     |
| ------- | -------------------------- | --------------------- | ------- | ---------- | ---------------------- | ------------- |
```

Each entry (row) in the sample dataset gives the following features:
- `time`: The time the measurement was taken.
- `value`: The measured value corresponding to the sensor taking the reading.
- `field`: The field for the measured value. For encoders this indicates which cartesian coordinate the measurement is taken in (e.g. [`x`,`y`,`z`]) and for the `load_cell` (i.e. force measuring device) this gives the direction of the force vector in cartesian coordinates (e.g. [`fx`,`fy`,`fz`]).
- `robot_id`: The ID number for the robot being used (i.e. there are 2 robots so the `robot_id` is either `1` or `2`).
- `run_uuid`: A random ID number assigned to the part being formed.
- `sensor_type`: The type of sensor reporting data (e.g. encoder = robot position / load_cell = force measurements)

Note: The encoder gives all values in millimeters and the load cell gives all values in Newtons.

## 2. Instructions
Design an ETL pipeline for processing the timeseries data given in `data/sample.parquet`. Your pipeline should perform the following steps:

1. Preprocess and Clean
2. Convert the timeseries data into a "wide" format
3. Include Engineered/Calculated Features
4. Calculate Runtime Statistics
5. Store and Provide tools for easily accessing this processed data.

All of these results should be saved in a format that is easy for others to access. It's up to you how you want to store things. You can use simple CSV files, a SQL database, etc. Regardless what you choose, make sure the structure is simple enough that any stakeholder wanting to use your data could easily read it back using the tools and methods you provide. 

### 2.1 Preprocess and Clean

Your first task in the ETL pipeline should be to extract/read data from `sample.parquet` and pre-process/clean the data (e.g. handling NaN values, expected values, outliers). You should also consider methods for enforcing datatypes, filling missing data, etc so that your pipeline doesn't break. If one of the sensors was missing some data, would your pipeline fail? Does your pipeline assume a certain structure for the data? How does it accomodate changes or mistakes down stream from where your ETL pipeline runs? Use your best judgement given what you understand about the type of data given what cleansing steps make sense.
### 2.2 Convert timeseries to a wide format
#### Convert to features
Timeseries is a convenient format for storing data but it's often not the most useful for interacting with, making plots, training ML models, etc. With your newly processed data, convert the timeseries data into a wide format that has the encoder values (`x`,`y`,`z`) and forces (`fx`,`fy`,`fz`) for each of the two robots (1,2) as individual columns. So rather than each row showing a time, sensor_type, robot_id, etc the data should show measurements for robots 1 & 2 corresponding to each encoder (e.g. `x_1`, `y_1`, `z_1`, `x_2`, `y_2`, `z_2`) and forces (`fx_1`, `fy_1`, `fz_1`, `fx_2`, `fy_2`, `fz_2`) for every timestamp. 

In the end, the header for your data should look like the following:

```
|------|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|-------|
| time |  fx_1  |  fx_2  |  fy_1  |  fy_2  |  fz_1  |  fz_2  |  x_1   |  x_2   |  y_1   |  y_2   |  z_1   |  z_2  |
|------|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|-------|
| 1234 | 0.1111 | 0.2222 | 0.3333 | 0.4444 | 0.5555 | 0.6666 | 0.7777 | 0.8888 | 0.9999 | 0.1234 | 0.5678 | 0.987 | 
|------|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|-------|
```

#### Match timestamps with measurements
After converting the timeseries data to individual features, you'll notice many gaps in the data (like in the table shown below). This happens because encoder and load cell sensors run independently and their time stamps are not in sync with one another. Additionally, because we have two separate robots (each with their own encoder and load cell sensors) you end up with 4 separate sets of measurements with their own unique timestamps. 
1. Robot 1 encoder measurements
2. Robot 1 load cell measurements
3. Robot 2 encoder measurements
4. Robot 2 load cell measurements

However, we would like to be able to compare each of these features corresponding to a single timestamp. So in this step of the ETL, you should transform the data to guarantee that any timestamp we access has a value for each column. Regardless which strategy you choose, explain why you chose it and what benefits or trade-offs it involves.

This data should also be saved in a way that these tables can be accessed by reference to the `run_uuid`. The table below is an example of data for `run_uuid = 6176976534744076781` with the "gaps" in the data you will need to transform. 

```
|----------------------------|---------------|---------------|---------------|----------------|----------------|---------------|-----------|------------|-----------|-----------|-----------|-------|
|           time             |     fx_1      |     fx_2      |     fy_1      |     fy_2       |     fz_1       |     fz_2      |    x_1    |    x_2     |    y_1    |    y_2    |    z_1    |  z_2  |
| -------------------------- | ------------- | ------------- | ------------- | -------------- | -------------- | ------------- | --------- | ---------- | --------- | --------- | --------- | ----- |
| 2022-11-23T20:40:00.007Z   | 176.0963814   |               | 174.2686233   |                | -258.1794165   |               |           |            |           |           |           |       |
| -------------------------- | ------------- | ------------- | ------------- | -------------- | -------------- | ------------- | --------- | ---------- | --------- | --------- | --------- | ----- |
| 2022-11-23T20:40:00.008Z   |               |               |               |                |                |               |           | 1438.412   |           | 939.383   |           |       |
| -------------------------- | ------------- | ------------- | ------------- | -------------- | -------------- | ------------- | --------- | ---------- | --------- | --------- | --------- | ----- |
| 2022-11-23T20:40:00.011Z   |               |               |               |                |                |               | 1440.79   |            | 936.925   |           | -222.29   |       |
| -------------------------- | ------------- | ------------- | ------------- | -------------- | -------------- | ------------- | --------- | ---------- | --------- | --------- | --------- | ----- |
| 2022-11-23T20:40:00.017Z   | 178.4532845   |               | 172.338417    |                | -259.4669531   |               |           |            |           |           |           |       |
| -------------------------- | ------------- | ------------- | ------------- | -------------- | -------------- | ------------- | --------- | ---------- | --------- | --------- | --------- | ----- |
| 2022-11-23T20:40:00.01Z    |               | 50.37302734   |               | -416.0604053   |                | 80.69138062   |           |            |           |           |           |       |
| -------------------------- | ------------- | ------------- | ------------- | -------------- | -------------- | ------------- | --------- | ---------- | --------- | --------- | --------- | ----- |
| 2022-11-23T20:40:00.023Z   |               |               |               |                |                |               | 1440.79   |            | 936.925   |           | -222.29   |       |
| -------------------------- | ------------- | ------------- | ------------- | -------------- | -------------- | ------------- | --------- | ---------- | --------- | --------- | --------- | ----- |
```

### 2.3 Include Engineered/Calculated Features

Recorded values are great but often times we want to calculate some other useful information. In the previous step you created a more convenient format for our encoder and force data. Take this information and generate some engineered features. You should include the follwing features for both robots (1 & 2):
- 6 Velocity values (`vx_1`, `vy_1`, `vz_1`, `vx_2`, `vy_2`, `vz_2`)
- 6 Acceleration values (`ax_1`, `ay_1`, `az_1`, `ax_2`, `ay_2`, `az_2`)
- Total Velocity (`v1`, `v2`)
- Total Acceleration (`a1`, `a2`)
- Total Force (`f1`, `f2`)

### 2.4 Calculate Runtime Statistics

Produce a dataset that provides useful statistics about the different `run_uuid` information. Your statistics data should include the following:
- `run_uuid`
- run start time
- run stop time
- total runtime
- total distance traveled 

### 2.5 Store and Provide Access Tools

In the end, all of this information should be saved somewhere that is easy for other people to access. You can decide how you prefer to store this information. Maybe you prefer a SQL database. Maybe you prefer a tabular format. Choose the approach that you think makes the most sense for this specific setup. You can also choose how you want this information distributed. Do you think it should all be saved in one location? Should it be saved across multiple separate data stores? 

## Tips & Advice

Try to follow ETL best practices for your example code. Good habits that we like to see might include:
- Making your tasks modular and easy to iterate on or modify. Adding a new task into the pipeline shouldn't break the rest of the ETL flow.
- Enforce datatypes and include exception handling and data cleaning measures for dealing with errors.
- Consider a design for your workflow that would make it easy to modify or update data as new features get added. If you put all of the data in one location, how easy would it be to udpate or modify. If you spread your data across multiple locations, how would updates or modifications propagate to all those locations? 
- Consider processing speed and use parallel processing for independent tasks when possible. Which parts of your pipeline can be parallelized? Which have to be done sequentially?
- Save data in formats that are easily extensible and convneint to query.
- Consider how your solution scales. If your implementation doesn't scale well, which parts would need the most/least modifcation? 

## Conclusion 

There is a lot of information contained in this Readme. Most of the suggestions are based on experiences we've had designing our own ETL process. However we are interested in seeing how you approach these problems. So feel free to deviate from these suggestions where you think it's appropriate. Also, we strongly encourage you to include comments and descriptions of your thought process in your final solution. If you would like to submit a readme or document with notes and explanations for your ETL we will gladly take that into account when reviewing your code.

# Submission
In order to submit the assignment you may either push your code/results to github (or gitlab, bitbucket, etc) and share the link with us or send a zipped folder containing your solution via email. 

5. Develop your homework solution in the cloned repository and push it to Github when you're done. Extra points for good Git hygiene.

6. Send us the link to your repository.


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


Requirement
1. Install Docker https://docs.docker.com/engine/install/, tested with community edition with 8G memory Docker Desktop 4.33.0, DockerCompose v2.14.0 or higher on MacOS

Commands
1. `docker compose up airflow-init` (no need)
2. `docker compose up`
3. `./airflow.sh bash`
4. `http://localhost:8080` 
5. `docker compose down --volumes --rmi all`