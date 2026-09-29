from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("store_id")

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.sum("units").alias("total_units"))
    .withColumn("cumulative_units", F.sum("total_units").over(w))
    .orderBy(F.col("cumulative_units").desc())
)
