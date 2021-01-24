# Data Engineering Capstone Project

## Project Summary

The purpose of this project is an attempt to combine technologies learned throughout the Udacity Data Engineering program. The project encompasses four datasets. The main dataset includes data on immigration to the United States, and supplementary datasets include data on airport codes, U.S. city demographics, and temperature data.

The project follows the follow steps:
Step 1: Scope the Project and Gather Data
Step 2: Explore and Assess the Data
Step 3: Define the Data Model
Step 4: Run ETL to Model the Data
Step 5: Complete Project Write Up

## Step 1: Scope the Project and Gather Data

### Scope

This a capstone project and it's a part of the Udacity Data Engineering Nanodegree program. This project mimics a real-life situation when you need to analyze, clean, save the data into a columnar file format and load the data to a data lake on S3 using Spark. Create an Airflow pipeline to load the data to the Redshift database for further analytical purposes. You can see the process below.

### Describe and Gather Data

The project contains four files that were gathered and provided by Udacity.
I94 Immigration Data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. There is a link to the original dataset https://travel.trade.gov/research/reports/i94/historical/2016.html. A sample dataset was along provided in the workspace (immigration_data_sample.csv). The data dictionary can be found in I94_SAS_Labels_Descriptions.SAS file.

### I94 Data Dictionary

* cicid - float64 - ID that uniquely identify one record in the dataset
* i94yr - float64 - 4 digit year
* i94mon- float64 - Numeric month
* i94cit - float64 - 3 digit code of source city for immigration (Born country)
* i94res - float64 - 3 digit code of source country for immigration (Residence country)
* i94port - object - Port addmitted through
* arrdate - float64 - Arrival date in the USA
* i94mode - float64 - Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = Not reported)
* i94addr - object - State of arrival
* depdate -float64 - Departure date
* i94bir - float64 - Age of Respondent in Years
* i94visa - float64 - Visa codes collapsed into three categories: (1 = Business; 2 = Pleasure; 3 = Student)
* count - float64 - Used for summary statistics
* dtadfile - object - Character Date Field
* visapost - object - Department of State where where Visa was issued
* occup - object - Occupation that will be performed in U.S.
* entdepa - object - Arrival Flag. Whether admitted or paroled into the US
* entdepd - object - Departure Flag. Whether departed, lost visa, or deceased
* entdepu - object - Update Flag. Update of visa, either apprehended, overstayed, or updated to PR
* matflag - object - Match flag
* biryear - float64 - 4 digit year of birth
* dtaddto - object - Character date field to when admitted in the US
* gender - object - Gender
* insnum - object - INS number
* airline - object - Airline used to arrive in U.S.
* admnum - float64 - Admission number, should be unique and not nullable
* fltno - object - Flight number of Airline used to arrive in U.S.
* visatype - object - Class of admission legally admitting the non-immigrant to temporarily stay in U.S.

World Temperature Data comes from Kaggle. Further details about the dataset can be found here: https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data.
