from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_units").desc())

result = (
    df_store_sales
    .filter(F.col("revenue") < 2000)
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
    .withColumn("units_rank", F.rank().over(w))
    .orderBy(F.col("units_rank").desc())
)
