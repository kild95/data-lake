# Data Lake with Spark

## Overview
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


## Project Structure

The repository is made up of the following files:

```
.
├── dl.cfg                   # Configuration file containing AWS IAM credentials
├── etl.py                   # Extracts data from S3 and processes using Spark
├── etl_emr.py               # Script to deploy to EMR
└── README.md

```

## Data Lake Schema
This project implements a star schema. `songplays` is the fact table in the data model, while `users`, `songs`, `artists`, and `time` are all dimensional tables.

### Fact Table
* **`songplays`** - records in event data associated with song plays (records with page = NextSong)
  * `start_time`, `userId`, `level`, `sessionId`, `location`, `userAgent`, `song_id`, `artist_id`, `songplay_id`

### Dimensional Tables
* **users** - users of the Sparkify app.
  * `firstName`, `lastName`, `gender`, `level`, `userId`
* **songs** - collection of songs.
  * `song_id`, `title`, `artist_id`, `year`, `duration` 
* **artists** - information about artists.
  * `artist_id`, `artist_name`, `artist_location`, `artist_lattitude`, `artist_longitude`
* **time** - timestamps of records in songplays, deconstructed into various date-time parts.
  * `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`


## How to Run
1. Add appropriate AWS IAM Credentials in `dl.cfg` (without quotations)
2. Specify desired output data path in the `main` function of `etl.py`
3. Run `etl.py`

## Extra Notes
When creating the spark session with the code provided by Udacity, I was getting a bad gateway due to `dl.bintray.com` being inaccessible. I had to download the  [aws](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.0) jar and rewrite some of the code. More info can be found [here](https://knowledge.udacity.com/questions/886926). For this reason, the aws jar is included in this repo.

## AWS
I had issues with the Udacity AWS Cloud Gateway, so I moved to my personal AWS account. The rest of this section gives guidance on how I worked things from my personal AWS account.

### AWS CLI
To get up and running with AWS CLI, follow the steps [here](https://classroom.udacity.com/nanodegrees/nd027/parts/67bd4916-3fc3-4474-b0fd-197dc014e709/modules/3671171a-8bf9-4f75-a913-37439ad281b3/lessons/f08d8ac9-b44a-4717-9da1-9714829da6c9/concepts/0f8c0f7f-f5f1-4dc8-bc72-97dfbb6e0eab). Be careful to use the correct profile when trying to writing `aws` commands. For example, if you want to list the contents of a (public) S3 bucket, I had to use the udacity_dataeng profile I set up because I needed to be in us-east-1 region and not us-west-2 region (which was set in my default profile). Note that when I did not set the correct profile using `--profile`, I was getting an expired token issue. Therefore I used the new profile I had set up which used new keys that I had created under a new IAM with admin access and used that to access the S3 bucket (even though I'm not sure if IAM keys were needed since the bucket was public). 

Useful [commands](https://docs.aws.amazon.com/cli/latest/reference/s3/ls.html ) to list contents of S3 bucket include:

* `aws s3 ls s3://udacity-dend/song-data/ --profile udacity_dataeng`
* `aws s3 ls s3://udacity-dend/song-data/ --profile udacity_dataeng --recursive --human-readable --summarize`

### EMR

#### Submitting Spark Job
Using `etl_emr.py`, I had issues with adding the script as a step to the EMR cluster which I was not able to resolve. My workaround was to copy to ssh into the cluster, place the script on the cluster itself, and run it using `spark-submit`:
1. ssh into cluster
2. Place the script onto the cluster itself - I manually created a file on the cluster and copied the code into it
3. Run the file using `spark-submit <filename>.py`
    
    