from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_age").desc())

result = (
    df_customers
    .groupBy("is_active")
    .agg(F.sum("age").alias("total_age"))
    .withColumn("age_rank", F.dense_rank().over(w))
)
