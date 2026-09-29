from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").asc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
w3 = Window.partitionBy("product_id").orderBy("revenue", "id")
w4 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("revenue_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id", "store_id").orderBy("revenue_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_store_sales
    .filter(F.col("product_id") == 5)
    .select("*", F.round((F.col("units") * 1.1), 2).alias("units_adjusted"))
)

deduped = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .filter(F.col("revenue").between(2200, 3700))
    .select(
        "id",
        "store_id",
        "product_id",
        F.percent_rank().over(w2).alias("revenue_percent_rank"),
        F.ntile(4).over(w3).alias("revenue_bucket"),
        F.last("units_adjusted", ignorenulls=True).over(w4).alias("last_known_units_adjusted"),
        F.sum("units_adjusted").over(w5).alias("running_units_adjusted"),
    )
)

metrics_store_sales = (
    metrics
    .withColumn("running_max_revenue_bucket", F.max("revenue_bucket").over(w6))
    .withColumn("running_sum_revenue_bucket", F.sum("revenue_bucket").over(w6))
    .withColumn("running_avg_revenue_bucket", F.avg("revenue_bucket").over(w6))
    .withColumn("cumulative_running_units_adjusted", F.sum("running_units_adjusted").over(w7))
)

valid = (
    metrics_store_sales
    .filter(F.col("product_id") == 8)
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue_percent_rank",
        "revenue_bucket",
        "running_max_revenue_bucket",
        "running_sum_revenue_bucket",
        "running_avg_revenue_bucket",
        "cumulative_running_units_adjusted",
        F.round("running_avg_revenue_bucket", 0).alias("running_avg_revenue_bucket_rounded"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.stock_qty"), F.col("d.product_name"))
)

result = (
    with_dims
    .groupBy("product_id")
    .agg(
        F.round(F.avg("cumulative_running_units_adjusted"), 2).alias("avg_cumulative_running_units_adjusted"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("running_max_revenue_bucket"), 2).alias("avg_running_max_revenue_bucket"),
        F.percentile_approx("running_avg_revenue_bucket", 0.5).alias("running_avg_revenue_bucket_p50"),
        F.percentile_approx("cumulative_running_units_adjusted", [0.25, 0.5, 0.75]).alias("cumulative_running_units_adjusted_quartiles"),
        F.count("*").alias("event_count"),
    )
)
