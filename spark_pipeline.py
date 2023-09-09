# https://community.cloud.databricks.com/?o=2253556103119442#

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

spark = SparkSession.builder\
        .master("local")\
        .appName("Pipeline")\
        .getOrCreate()

newDF = [
    StructField("id", IntegerType(), True),
    StructField("text", StringType(), True),
    StructField("label", DoubleType(), True)]
finalSchema = StructType(fields=newDF)
dataset = spark.read.format('csv').options(header='true',schema=finalSchema,delimiter='|').load('dataset.csv')

dataset = dataset.withColumn("label", dataset["label"].cast(DoubleType()))
dataset = dataset.withColumn("id", dataset["id"].cast(IntegerType()))
training, test = dataset.randomSplit([0.8, 0.2], seed=12345)

tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol=remover.getOutputCol(), outputCol="features").setNumFeatures(4096)

lr = LogisticRegression(maxIter=2, regParam=0.001)
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, lr])

# Fit the pipeline to training documents.
model = pipeline.fit(training)
result = model.transform(test)\
    .select("features", "label", "prediction")
correct = result.where(result["label"] == result["prediction"])
accuracy = correct.count()/test.count()
print("Accuracy of model = "+str(accuracy))
test_error = 1 - accuracy
print ("Test error = "+str(test_error))
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
metric = evaluator.evaluate(result)
print("F1 metric = "+ str(metric))

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
metric = evaluator.evaluate(result)
print("Recall = "+ str(metric))

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
metric = evaluator.evaluate(result)
print("Precision = "+ str(metric))
model.save("lrmodel")