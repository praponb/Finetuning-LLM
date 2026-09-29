from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_age").desc())

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.sum("age").alias("total_age"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy("country")
)
