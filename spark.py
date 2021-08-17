#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install cassandra-driver


# In[ ]:


get_ipython().system('pip install spark-nlp==3.1.2')


# In[ ]:


import pandas as pd
import json
import pprint
from datetime import datetime, timezone


# In[ ]:


from cassandra.cluster import Cluster

cluster = cluster = Cluster(['cassandra'], port=9042)  # provide contact points and port
session = cluster.connect()
session.set_keyspace('twitt')


# In[ ]:


from sparknlp.base import *
from sparknlp.annotator import *
import sparknlp

spark = sparknlp.start()


# In[ ]:


from pyspark import SparkContext, SparkConf, SQLContext
import pyspark.sql.functions as F

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)


# In[ ]:


rows = session.execute('select * from hashtags')

rdd = sc.parallelize(list(rows))
df = sqlContext.createDataFrame(rdd)
df.registerTempTable('hashtags')
df.toPandas(()


# In[ ]:


df.printSchema()


# In[ ]:


sqlContext.sql("select count(*) from hashtags").show()


# In[ ]:


sqlContext.sql("""select count(hashtag) as recurrence, hashtag 
                from hashtags 
                group by hashtag 
                order by recurrence desc""").toPandas()


# In[ ]:


df2 = sqlContext.sql("""select ROW_NUMBER() over (ORDER BY id) AS id, hour, minute, second
                from hashtags
                where hashtag='ایران'""")

df2.registerTempTable('hashtag_time')


# In[ ]:


df2.toPandas()


# In[ ]:


import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')

df3 = df2.groupBy('hour').count()
df3pandas = df3.toPandas()
df3pandas.plot(kind='bar', x='hour', y='count', title='hashtag per hour')


# In[ ]:


df3 = sqlContext.sql("""select t1.id, t1.hour, t1.minute, t1.second, (t1.hour*3600+t1.minute*60+t1.second) as timestamp, (t2.hour-t1.hour)*3600+(t2.minute-t1.minute)*60+(t2.second-t1.second) as next
                from hashtag_time as t1 join hashtag_time as t2
                where t2.id = t1.id+1""")

df3.registerTempTable('hashtag_timestamp')

df3.toPandas()


# In[ ]:


from pyspark.ml.feature import VectorAssembler

vectorAssembler = VectorAssembler(inputCols = ['timestamp'], outputCol = 'features')
model_df = vectorAssembler.transform(df3)
model_df = model_df.select(['features', 'next'])
model_df.toPandas()


# In[ ]:


splits = model_df.randomSplit([0.8, 0.2])
train_df = splits[0]
test_df = splits[1]

print("Train count: " + str(train_df.count()))
print("Test count: "+ str(test_df.count()))


# In[ ]:


from pyspark.ml.regression import GBTRegressor
from pyspark.sql.functions import round

gbt = GBTRegressor(featuresCol = 'features', labelCol='next', maxIter=10)
gbt_model  = gbt.fit(train_df)

gbt_predictions = gbt_model.transform(test_df)

gbt_predictions = gbt_predictions.select('prediction', round(F.col('prediction'), 0).alias('pred_round'), 'next', 'features')
gbt_predictions.toPandas()


# In[ ]:


pandas_pred = gbt_predictions.toPandas()
pandas_pred.plot(x='features', y=['prediction', 'next'], title='GBT Predictions on Test')
pandas_pred.plot(x='features', y=['pred_round', 'next'], title='Rounded Predictions')


# In[ ]:


from pyspark.ml.evaluation import RegressionEvaluator

gbt_evaluator = RegressionEvaluator(
    labelCol="next", predictionCol="prediction", metricName="rmse")
rmse = gbt_evaluator.evaluate(gbt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)


# In[ ]:


test_df.describe().toPandas()

