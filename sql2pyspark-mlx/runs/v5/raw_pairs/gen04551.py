from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_sqft").desc())

result = (
    df_stores
    .filter(F.col("sqft") < 5630)
    .groupBy("city")
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("sqft_rank", F.rank().over(w))
    .orderBy(F.col("total_sqft").desc())
)
