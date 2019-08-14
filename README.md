# 🛦 Concorde 🛦 - Airflow Plugin for Digital Marketers #

## What is Concorde? ##
Concorde is an open-source Airflow plugin, allowing marketers to ETL data from their various systems (such as Google Ads) into a storage solution (such as Amazon S3) for further analysis and/or processing.

Once in S3, the data can be:
* Analyzed in its raw format using AWS Athena
* Processed using an AWS Lambda function or using another Airflow DAG
* Uploaded into a Data Warehousing solution, such as AWS Redshift

Currently, only Google Ads (formerly Google AdWords) is supported, but work is ongoing to include data from Facebook Ads, Bing Ads, Salesforce, and Hubspot.

## Why use Concorde? ##
On one extreme, while Enterprise ETL solutions are well built and provide exceptional support, they can be prohibitively expensive, accessible only to those with the deepest pockets. On the other extreme, building your own solution often results in incurring massive amounts of technical debt that last long after original creators leave a company.

Concorde is a middle ground - leveraging Airflow's open-source framework to build an ETL solution robust enough for businesses, yet easy to implement and accessible to the masses. The source code is intended to be modular and easy to read, allowing for anyone to repurpose the code to suit their own projects.

## How to Connect: ##

To create a connection using the Airflow UI

Google Ads
Extra Params:
{
    developer_token
    client_customer_id
    user_agent
    client_id
    client_secret
    refresh_token
}

## How to run locally (using Localstack): ##
* Open up a Unix terminal to your working directory 
* Make sure you have docker and docker-compose installed
* Run the docker-compose file
* Create an S3 bucket on your localstack server using the following command: 
    * aws --endpoint-url=http://localhost:4572 s3 mb s3://[Bucket Name]
* Access the Airflow UI at http://localhost:8080
* Go to Admin > Connections, and enter your connection information for the services you want to test
* Use one of the example DAGs provided, or create your own DAG to test the plugin
* Test the DAG using the following command: 
    * docker exec -ti airflow-webserver airflow test [DAG Name] [Task Name] [Task Date]
* For example, using an example DAG for Google Ads, the command would be: 
    * docker exec -ti airflow-webserver airflow test **[DAG Name] [Task Name] [Task Date]**
* View the processed data via the emulated S3 buckets at http://localhost:4572
* Download the processed data using the following command:
    * aws --endpoint-url=http://localhost:4572 s3 cp s3://[Bucket Name] . --recursive

## How to use in Production: ##

## Next Steps: ##
- [ ] Implement operators and/or hooks for Facebook Ads, Bing Ads, Salesforce, and Hubspot
- [ ] Include support for different types of storage (HDFS, etc)
- [ ] Take care of issue post-processing Null values and "> 90 %" / "< 10 %" values 
- [ ] Refactor the operator so that you inherit from a base class (so you can reuse base class for different operators)

## The crew in the cockpit ##
Concorde was designed by Andrew Alkhouri (@andyalky) through work done at Campaigner (campaigner.com). You can connect with him on LinkedIn at www.linkedin.com/in/andyalky.