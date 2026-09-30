from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("running_max_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("running_max_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_store_sales
    .filter(F.col("product_id") == 6)
    .select(
        "id",
        "store_id",
        F.max("revenue").over(w).alias("running_max_revenue"),
        F.min("revenue").over(w).alias("running_min_revenue"),
        F.avg("revenue").over(w).alias("running_avg_revenue"),
        F.last("revenue", ignorenulls=True).over(w2).alias("last_known_revenue"),
    )
)

scored = (
    with_windows
    .withColumn("cumulative_running_min_revenue", F.sum("running_min_revenue").over(w3))
    .withColumn("running_running_min_revenue", F.sum("running_min_revenue").over(w4))
)

base = (
    scored
    .filter(F.col("running_max_revenue") <= 3100)
    .select(
        "id",
        "running_max_revenue",
        "running_avg_revenue",
        "last_known_revenue",
        "cumulative_running_min_revenue",
        "running_running_min_revenue",
        F.round((F.col("cumulative_running_min_revenue") / F.col("running_avg_revenue")), 2).alias("cumulative_running_min_revenue_per_running_avg_revenue"),
    )
)

base_store_sales = (
    base
    .filter(F.col("running_avg_revenue") <= 1200)
    .withColumn("running_running_min_revenue_filled", F.coalesce("running_running_min_revenue", F.lit(0)))
    .withColumn("running_avg_revenue_plus_cumulative_running_min_revenue_per_running_avg_revenue", (F.col("running_avg_revenue") + F.col("cumulative_running_min_revenue_per_running_avg_revenue")))
)

cleaned = (
    base_store_sales
    .filter(F.col("running_running_min_revenue") > 15004)
    .withColumn("greatest_running_running_min_revenue_running_max_revenue", F.greatest("running_running_min_revenue", "running_max_revenue"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("running_avg_revenue_plus_cumulative_running_min_revenue_per_running_avg_revenue") >= 65, "LARGE").otherwise("SMALL").alias("running_avg_revenue_plus_cumulative_running_min_revenue_per_running_avg_revenue_tier"),
        F.when(F.col("cumulative_running_min_revenue_per_running_avg_revenue") >= 84, "LARGE").otherwise("SMALL").alias("cumulative_running_min_revenue_per_running_avg_revenue_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.max("running_avg_revenue").alias("max_running_avg_revenue"),
        F.percentile_approx("cumulative_running_min_revenue", [0.25, 0.5, 0.75]).alias("cumulative_running_min_revenue_quartiles"),
    )
    .filter(F.col("max_running_avg_revenue") > 0)
    .orderBy(F.col("max_running_avg_revenue").desc())
)
