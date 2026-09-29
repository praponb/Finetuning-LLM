from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock_month_end").desc_nulls_last(), "id")
w_running = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("quantity", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("quantity_bucket").desc(), "id")

valid = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .select("*", F.last_day("last_restock").alias("last_restock_month_end"))
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "quantity", "last_restock_month_end")
)

metrics = (
    ranked
    .select(
        "id",
        "store_id",
        F.min("quantity").over(w_running).alias("last7_min_quantity"),
        F.max("quantity").over(w_running).alias("last7_max_quantity"),
        F.count("quantity").over(w_running).alias("last7_count_quantity"),
        F.ntile(4).over(w3).alias("quantity_bucket"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy("store_id")
    .agg(
        F.percentile_approx("last7_count_quantity", [0.25, 0.5, 0.75]).alias("last7_count_quantity_quartiles"),
        F.percentile_approx("last7_count_quantity", [0.25, 0.5, 0.75]).alias("last7_count_quantity_quartiles_2"),
        F.percentile_approx("quantity_bucket", 0.9).alias("quantity_bucket_p90"),
        F.min("last7_count_quantity").alias("min_last7_count_quantity"),
        F.percentile_approx("last7_min_quantity", 0.9).alias("last7_min_quantity_p90"),
    )
    .filter(F.col("min_last7_count_quantity") < 1000000)
    .orderBy(F.col("quantity_bucket_p90").desc_nulls_first())
)
