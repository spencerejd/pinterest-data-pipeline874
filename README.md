# Pinterest Data Pipeline

## Table of Contents
1. [Project Description](#project-description)
    1. [What It Does](#what-it-does)
2. [Installation instructions](#installation-instructions)
3. [Usage instructions](#usage-instructions)
4. [File structure of the project](#file-structure-of-the-project)
5. [Licence information](#licence-information)

## Project Description
This project replicates a data pipeline similar to the one used by Pinterest for processing billions of data points daily. In this project, the data pipeline created encompasses various stages including batch processing, stream processing and data transformation and analysis using tools such as Apache Kafka Amazon S3, AWS Kinesis and Databricks. This provides hands-on experience with these technologies, emulating a real-world data engineering scenario.

## Technologies used
- [**Apache Kafka**](https://kafka.apache.org/)
> Apache Kafka is an open-source distributed event streaming platform well suited for handling high-throughput, fault-tolerant handling of data pipelines.

- [**Amazon EC2**](https://aws.amazon.com/ec2/)
> Amazon Elastic Compute Cloud offers scalable computing capacity in the cloud. It enables you to launch EC2 instances which represent virtual servers in the cloud, running on AWS infrastructure.

- [**AWS MSK**](https://aws.amazon.com/msk/)
> Amazon Managed Streaming for Apache Kafka is a fully managed service that enables you to build and run applications that use Apache Kafka to process streaming data.

- [**AWS MSK Connect**](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect.html)
> AWS MSK Connect simplifies the connecting of Apache Kafka applications to other AWS services. Makes it easy to create, manage and scale Kafka connectors.

- [**Kafka REST Proxy**](https://docs.confluent.io/platform/current/kafka-rest/index.html)
> The Kafka REST Proxy provides an interface for producing and consuming Apache Kafka messages over HTTP.

- [**AWS API Gateway**](https://aws.amazon.com/api-gateway/)
> AWS API Gateway is a fully managed service that makes it easy to create, publish, maintain, monitor and secure APIs at any scale.

- [**Apache Spark**](https://spark.apache.org/)
> Apache Spark is an open-source distributed multi-language engine for executing data engineering, data science and machine learning on single-node machines or clusters.

- [**Databricks**](https://www.databricks.com/)
> Databricks is a cloud-based platform designed for big data analytics and artificial intelligence. It is built on top of Apache Spark. It offers integrations with various data sources and simplifies the management of infrastructure to make it easier to derive insights from data.

- [**PySpark**](https://spark.apache.org/docs/latest/api/python/index.html)
> PySpark is the Python API for Apache Spark which enables you to perform real-time, large-scale data processing in a distributed environment using Python.

[**AWS MWAA**](https://aws.amazon.com/managed-workflows-for-apache-airflow/)
> Amazon Managed Workflows for Apache Airflow is a managed service that makes it easy to orchestrate complex data workflows using Apache Airflow. It handles the provisioning, setup and maintenance of Airflow environments.

[**AWS Kinesis**](https://aws.amazon.com/kinesis/)
> AWS Kinesis is a managed service for the collection, processing and analysis of real-time data across a wide array of applications.



## ARCHITECTURE OVERVIEW
Slide with arrows showing the different platforms involved in this architecture



## Project Overview
The project will entail replicating Pinterest's end-to-end Data processing pipeline, leveraging both batch and stream processing. 
A script is used to emulate user posts from the Pinterest platform, which are sent to an API. This stream of data initially utilises MSK Connect, which facilitates the use of Kafka for its storage in an S3 bucket. Airflow is leveraged to extract batch data from the S3 bucket and stream it to Kinesis. The data processing pipeline is complete with the streamed data cleaned by Spark.

## Building the Pinterest Data Pipeline

### Generate Pinterest Data
Run the user_posting_emulation.py file in the terminal, which should emulate a stream of data generated at Pinterest which captures data around posts made to Pinterest, user data and geolocation data.

## Batch Processing

### Configure the EC2 Kafka Client

#### EC2 instance setup
An EC2 instance is first configured to act as a client to communicate with an Apache Kafka cluster in AWS.
To start with, you will need to navigate to the AWS EC2 dashboard to launch an EC2 instance. Give the instance a name and keep the default Application and OS Images and instance types. The other options available may require consideration on usage and cost.
[image](images/EC_Instance_Configuration.png)

Under "Network & Security" you will then create a Key Pair. This will generate a private key file (`.pem`) which will grant you a secure SSH connection to locally access your EC2 instance.
To connect to the EC2 instance, follow the SSH client
Follow the instructions outlined under 'SSH client' to connect to your instance.

#### Kafka Setup
Now, create the Apache Kafka cluster, which will use EC2 as a client for communication from your local device to AWS.
Open the AWS MSK dashboard and navigate to Create Cluster. Keep the default options (for this project we used the recommended 2.8.1 Apache Kafka version).
Navigate to the security group of your cluster's Virtual Private Cloud and edit the inbound rules to ensure that your client machine can send data to the MSK cluster.

With your EC2 client machine already connected, navigate to the directory where your Private key file is located and run the following commands to install Kafka on the client machine:

```
# install Java, required for Kafka
sudo yum install java-1.8.0
# download the same version of Kafka setup in the MSK Kafka cluster
wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
tar -xzf kafka_2.12-2.8.1.tgz
```

Next, install the IAM MSK authentication package onto your client EC2 machine:

```
# navigate to the correct directory
cd kafka_2.12-2.8.1/libs/
# download the package
wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.5/aws-msk-iam-auth-1.1.5-all.jar
```

Configure the client to use the IAM package:

```
# open bash config file
nano ~/.bashrc
```

Add the following line to the config file, then save and exit:

```
export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar

# activate changes to .bashrc
source ~/.bashrc
```

Lastly, to complete the Kafka client configuration, modify the client.properties file and exit:

```
# navigate to Kafka bin folder
cd ../bin
# create client.properties file
nano client.properties

# Sets up TLS for encryption and SASL for authN.
security.protocol = SASL_SSL

# Identifies the SASL mechanism to use.
sasl.mechanism = AWS_MSK_IAM

# Binds SASL client implementation.
sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required;

# Encapsulates constructing a SigV4 signature based on extracted credentials.
# The SASL client bound by "sasl.jaas.config" invokes this class.
sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

#### Create topics on the Kafka cluster
You can now proceed to create topics on the Kafka cluster using the client machine command line to complete EC2 Kafka configuration. You can retrieve the Bootstrap servers string from AWS MSK to create your topic. More information on this in the (AWS MSK documentation)[https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html].

```
<path-to-your-kafka-installation>/bin/kafka-topics.sh --create --bootstrap-server <BootstrapServerString> --command-config client.properties --topic <topic name>
```
In this project we created three topics to represent `pinterest_data`, `geolocation_data` and `user_data`.





### Data Ingestion
Kafka used to facilitate the storage of data from the script/API into an s3 bucket

Kafka REST Proxy essentially enables  data to flow from the script directly to the MSK cluster (?),
### Batch Processing
Airflow (MWAA) takes the data from S3 bucket 
### Stream Processing
Kinesis Data stream is read into Databricks and transformed


## Dataset
The project contains a script, <u>user_posting_emulation.py<u> that will mimic the stream of data points received by the Pinterest API following POST requests made by users uploading data onto Pinterest.
The script instantiates a database connector class, which is used to connect to a AWS RDS database which contains the following tables:
- `pinterest_data`


## Project Dependencies
To run the project, the following modules are required:
- `sqlalchemy`
- `requests`




## Installation instructions

## Usage instructions

## File structure of the project
The project is structured as follows:

`user_posting_emulation.py`: File emulates user posting behaviour for eventual batch processing

`user_posting_emulation_streaming.py`: Simulates user posting behavior for streaming data

`Milestone_7_Batch_Processing_Databricks.ipynb`: Databricks notebook for batch processing

`Milestone_9_Stream_Processing.ipynb`:  Databricks notebook for stream processing

`0a1667ad2f7f_dag.py` : Apache Airflow DAG file for workflow orchestration

`Mount_S3_Bucket.ipynb` : Databricks notebook for mounting S3 buckets

`README.md`: Documentation file providing information about the Pinterest Data Pipeline.


## Licence information
MIT License

Copyright (c) 2024 Spencer Duvwiama

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
