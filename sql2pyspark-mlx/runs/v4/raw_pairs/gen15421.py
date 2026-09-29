from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_quarter")

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.sum("age").alias("total_age"))
    .withColumn("cumulative_age", F.sum("total_age").over(w))
    .orderBy("signup_quarter")
    .limit(50)
)
