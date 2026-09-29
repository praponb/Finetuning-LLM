from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft_bucket")

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 13066, "high").when(F.col("sqft") >= 7736, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("cumulative_sqft", F.sum("total_sqft").over(w))
    .orderBy("sqft_bucket")
)
