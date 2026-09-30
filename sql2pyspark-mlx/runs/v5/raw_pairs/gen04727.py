from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc())

latest = (
    df_inventory
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy("store_id")
    .agg(
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.sum("quantity").alias("total_quantity"),
    )
)
