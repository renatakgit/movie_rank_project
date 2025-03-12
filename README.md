# Big Data Postgraduate Final Project

## Project Overview
This project was created as the final assignment for my postgraduate studies in Big Data. The goal was to process and analyze movie review data from Kaggle, clean the dataset, generate a movie ranking, and detect anomalies in user reviews.

## Architecture
The project follows a hybrid data processing approach using AWS services, combining batch and stream processing:

1. **Preprocessing:**
   - Raw JSON data is stored in an AWS S3 bucket.
   - AWS Glue processes the data and stores it in a formatted structure.
   - AWS Glue Data Catalog organizes metadata for efficient querying.
   - AWS Athena is used for data analysis and generating query results.

2. **Batch Processing:**
   - An Amazon EMR cluster runs PySpark scripts to process the cleaned data.
   - The processed results, including the movie ranking, are stored in S3.

3. **Stream Processing:**
   - AWS Lambda processes sample review data in real time.
   - Amazon Kinesis streams reviews to detect suspicious activity.

## Technologies Used
- **AWS Services:** S3, Glue, Glue Data Catalog, Athena, EMR, Lambda, Kinesis
- **Programming & Tools:** PySpark, SQL
- **Data Source:** Kaggle movie reviews dataset

## Results
- A structured and cleaned dataset for movie rankings.
- Detection of anomalies in reviews using Kinesis streaming.

## Acknowledgments
This project was developed as part of my postgraduate coursework in Big Data. The dataset was sourced from Kaggle.

