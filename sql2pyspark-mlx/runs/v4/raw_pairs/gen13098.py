from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units_scaled").desc_nulls_first())
w3 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id", "product_id").orderBy("units_scaled", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("product_id").orderBy("units_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy(F.col("running_units_scaled_2").desc(), "id")
w7 = Window.partitionBy("store_id", "product_id").orderBy("units_scaled_dense_rank", "id")
w8 = Window.partitionBy("store_id").orderBy(F.col("units_scaled").desc_nulls_last())
w9 = Window.partitionBy("product_id")
discount_by_store_id = df_orders.groupBy("store_id").agg(F.avg("discount").alias("avg_order_discount"))

valid = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 7, 10]))
    .withColumn("units_scaled", F.round((F.col("units") * 0.85), 2))
)

cleaned = (
    valid
    .filter(F.col("revenue") <= 1800)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "units_scaled",
        F.col("units").cast("string").alias("units_str"),
    )
)

scored = (
    cleaned
    .withColumn("running_units_scaled", F.sum("units_scaled").over(w))
    .withColumn("units_scaled_dense_rank", F.dense_rank().over(w2))
    .withColumn("running_units_scaled_2", F.sum("units_scaled").over(w3))
    .withColumn("units_avg_2_prior", F.avg("units").over(w4))
    .withColumn("last_known_units", F.last("units", ignorenulls=True).over(w5))
)

scored_store_sales = (
    scored
    .select(
        "*",
        F.row_number().over(w6).alias("event_seq"),
        F.ntile(5).over(w7).alias("units_scaled_dense_rank_bucket"),
        F.percent_rank().over(w8).alias("units_scaled_percent_rank"),
        F.ntile(10).over(w7).alias("units_scaled_dense_rank_bucket_2"),
        F.round((F.col("last_known_units") / F.sum("last_known_units").over(w9)), 4).alias("last_known_units_share"),
    )
)

enriched = (
    scored_store_sales
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.brand"))
)

result = (
    enriched
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "units",
        "units_str",
        "running_units_scaled",
        "units_scaled_dense_rank",
        "running_units_scaled_2",
        "units_avg_2_prior",
        "event_seq",
        F.col("avg_order_discount"),
    )
)
