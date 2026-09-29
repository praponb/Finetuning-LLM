from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last(), "id")

ranked = (
    df_inventory
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("activity_month"))
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
    )
)
