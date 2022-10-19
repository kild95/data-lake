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
├── hadoop-aws-2.7.0.jar     # Support Integration with AWS
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
1. Add appropriate AWS IAM Credentials in `dl.cfg`
2. Specify desired output data path in the `main` function of `etl.py`
3. Run `etl.py`

## Extra Notes
When creating the spark session with the code provided by Udacity, I was getting a bad gateway due to `dl.bintray.com` being inaccessible. I had to download the  [aws](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.0) jar and rewrite some of the code. More info can be found [here](https://knowledge.udacity.com/questions/886926). For this reason, the aws jar is included in this repo.