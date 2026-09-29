from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_first())
w3 = Window.orderBy("running_min_revenue", "id")
w4 = Window.partitionBy("store_id")
w5 = Window.orderBy("running_sum_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_min_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id", "product_id").orderBy("running_sum_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("product_id").orderBy("running_corr_units_revenue_2", "id")
w9 = Window.orderBy("corr_units_revenue", "id")

with_windows = (
    df_store_sales
    .select(
        "*",
        F.min("revenue").over(w).alias("running_min_revenue"),
        F.max("revenue").over(w).alias("running_max_revenue"),
        F.sum("revenue").over(w).alias("running_sum_revenue"),
        F.dense_rank().over(w2).alias("revenue_dense_rank"),
    )
)

metrics = (
    with_windows
    .withColumn("next_running_max_revenue", F.lead("running_max_revenue", 2).over(w3))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w4)), 4))
    .withColumn("corr_units_revenue", F.corr("units", "revenue").over(w4))
)

base = (
    metrics
    .filter(F.col("sale_date") > F.lit("2023-02-21").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "running_min_revenue",
        "running_sum_revenue",
        "next_running_max_revenue",
        "revenue_share",
        "corr_units_revenue",
        F.round("running_max_revenue", 2).alias("running_max_revenue_rounded"),
    )
)

with_windows_store_sales = (
    base
    .withColumn("running_corr_units_revenue", F.sum("corr_units_revenue").over(w5))
    .withColumn("running_corr_units_revenue_2", F.sum("corr_units_revenue").over(w6))
    .withColumn("cumulative_revenue_share", F.sum("revenue_share").over(w7))
)

metrics_store_sales = (
    with_windows_store_sales
    .select(
        "*",
        F.lag("running_sum_revenue", 1).over(w_hist).alias("prev_running_sum_revenue"),
        F.lead("corr_units_revenue", 2).over(w9).alias("next_corr_units_revenue"),
    )
)

with_dims = (
    metrics_store_sales
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.store_name").like("%Siam%"))
    .select("src.*", F.col("p.region"), F.col("p.city"))
)

result = (
    with_dims
    .groupBy("product_id")
    .pivot("city", ["London", "Berlin"])
    .agg(
        F.avg("cumulative_revenue_share").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "product_id",
        F.col("London_amount").alias("london_amount"),
        F.col("London_item_count").alias("london_item_count"),
        F.col("Berlin_amount").alias("berlin_amount"),
        F.col("Berlin_item_count").alias("berlin_item_count"),
    )
)
