from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 5]))
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_quantity") * 100) / F.sum("total_quantity").over(w)), 2))
)
