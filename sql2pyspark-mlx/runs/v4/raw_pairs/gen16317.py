from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc_nulls_last(), "id")

deduped = (
    df_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("week", "last_restock").alias("week_start"))
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.count("*").alias("event_count"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .filter(F.col("avg_quantity") < 0)
    .orderBy(F.col("avg_quantity").asc_nulls_last())
)
