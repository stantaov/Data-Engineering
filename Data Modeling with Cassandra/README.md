# Project: Data Modeling with Cassandra

The project was offered as part of Udacity's Data Engineering Nanodegree program.

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Project Overview

The goal of the project was to create a data model with NoSQL Apache Cassandra, create database and tables and load data. In this project, I applied what I learned on data modelling with Apache Cassandra and completed an ETL pipeline using Python. 

## ETL Pipeline

The part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables was provided. The provided project template takes care of all the imports and provides a structure for an ETL pipeline that I needed to process this data.

## Requested Data Analysis

I had to create the data tables in Apache Cassandra based on the queries and the queries are based on the customer's request for data. The completed data model can be examined in the Project_1B_Data_Modeling_with_Cassandra.ipynb Jupyter Notebook.

### Creating tables to run the following queries with Apache Cassandra.

1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
SELECT artist, song, lenght FROM test_db WHERE sessionId = 338 AND itemInSession = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
SELECT artist, song, fisrtName, lastName FROM test_db WHERE userid = 10 AND sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
SELECT fisrtName, lastName FROM test_db WHERE song = 'All Hands Against His Own'
