from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age_bucket")

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 72, "high").when(F.col("age") >= 33, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.sum("age").alias("total_age"))
    .withColumn("cumulative_age", F.sum("total_age").over(w))
    .orderBy(F.col("cumulative_age").desc())
)
