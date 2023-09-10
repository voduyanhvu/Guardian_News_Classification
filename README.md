# Guardian_News_Classification
This folder contains the code to run classification on streaming data.
We need to follow the given steps:

#### Step 1 : Data Extraction from Guardian APIs
Firsly, we need to generate data locally to train our model. We can use the local_data_collection.py and specify the dates and the key. This code will create the specified dataset(headers : id,text,label) and fix the category and labels file. The code will automatically increment the dates and keep on collecting the data specified by the iterations.

To use this API, you will need to sign up for API key here:<br/>
https://open-platform.theguardian.com/access/

There are multiple categories such as Australia news, US news, Football, World news, Sport, Television & radio, Environment, Science, Media, News, Opinion, Politics, Business, UK news, Society, Life and style, Inequality, Art and design, Books, Stage, Film, Music, Global, Food, Culture, Community, Money, Technology, Travel, From the Observer, Fashion, Crosswords, Law.

#### Step 2 : Create a model using the extracted data
After this I uploaded the dataset on the local PC and create the pipeline model using the spark_pipeline.py. This file will save the pipeline in a folder and will show the accuracy of the model by using dataset for 80% training and 20% testing.
To fix the error of Java heap memory, the feature dimension of HashingTF is confined to 4096.

#### Step 3 : Run stream_producer.py to use the extracted model to classify streaming data
Now we will using the stream_producer.py to create a kafka stream data and use the spark_stream.py file to read the stream and load the created model and generate the prediction on the fly. Please, add the saved model full path in the spark_stream.py file.

#### Python enviroment setup
download Miniconda3 Windows 64-bit: https://docs.conda.io/projects/miniconda/en/latest/<br/><br/>
create a pre-defined environment: conda env create -f py39_env.yml<br/><br/>
create a copy of the python executable file at miniconda3\envs\py39\python.exe as python3.exe<br/><br/>
 
#### Commands on a Windows-based machine
Spark version: spark-3.4.1-bin-hadoop3<br/><br/>
Kafka version: kafka_2.13-3.5.0<br/><br/>

1. start zookeeper : bin\windows\zookeeper-server-start.bat config\zookeeper.properties<br/><br/>

2. start kafka server : bin\windows\kafka-server-start.bat config\server.properties<br/><br/>

3. submit code : spark-submit.cmd --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1 spark_stream.py localhost:9092 test<br/><br/>

4. produce data : bin\windows\kafka-console-producer.sh --broker-list localhost:9092 --topic test<br/><br/>
