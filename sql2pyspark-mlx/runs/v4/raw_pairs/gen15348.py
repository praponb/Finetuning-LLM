from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("region")

result = (
    df_stores
    .filter(F.col("region").isin(["South", "Central"]))
    .groupBy("region")
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("cumulative_sqft", F.sum("total_sqft").over(w))
    .orderBy("total_sqft", "region")
    .limit(10)
)
