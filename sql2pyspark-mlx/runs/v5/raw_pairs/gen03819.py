from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_units").desc())

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.count("*").alias("cnt"),
    )
    .withColumn("units_rank", F.rank().over(w))
    .orderBy(F.col("units_rank").desc())
)
