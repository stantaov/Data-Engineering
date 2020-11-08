# Project: Data Modeling with Cassandra

The project was offered as part of Udacity's Data Engineering Nanodegree program.

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Project Overview

The goal of the project was to create a data model with NoSQL Apache Cassandra, create database and tables and load data. In this project, I applied what I learned on data modelling with Apache Cassandra and completed an ETL pipeline using Python. 

## ETL Pipeline

The part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables was provided. The provided project template takes care of all the imports and provides a structure for an ETL pipeline that I needed to process this data.

## Requested Data Analysis

I was instructed to create tables in Apache Cassandra based on the queries provided. The completed data model can be found in the Project_1B_ Project_Template.ipynb Jupyter Notebook.

### Creating tables to run the following queries with Apache Cassandra.

1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

SELECT artist, song, lenght FROM test_db WHERE sessionId = 338 AND itemInSession = 4

2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

SELECT artist, song, fisrtName, lastName FROM test_db WHERE userid = 10 AND sessionid = 182

3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

SELECT fisrtName, lastName FROM test_db WHERE song = 'All Hands Against His Own'

## Instruction to Run the Project with Docker Image

A Docker image was used so that I could develop this project on my local machine, rather than on Udacity's internal system. Thank you to Ken Hanscombe for providing easy to follow instructions on how to pull, run, and connect to the Apache Cassandra image.

With Docker installed, pull the latest Apache Cassandra image and run a container as follows:

> docker pull cassandra
> docker run --name cassandra-container -p 9042:9042 -d cassandra:latest

This will allow you to develop the data model (i.e. work through the Jupyter notebook), without altering the provided connection code which connects to the localhost with default port 9042:

> from cassandra.cluster import Cassandra
> cluster = Cluster(['127.0.0.1'])
> session = cluster.connect()

To stop and remove the container after the exercise:

> docker stop cssandra-container
> docker rm cassandra-container
