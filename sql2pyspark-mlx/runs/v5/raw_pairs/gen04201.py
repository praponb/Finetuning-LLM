from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.orderBy("last7_min_days_since_delivered_date", "id")
w4 = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("last7_max_days_since_delivered_date_tier").orderBy(F.col("avg_last7_min_days_since_delivered_date").desc())

base = (
    df_shipments
    .filter(~F.col("carrier").eqNullSafe(F.lit("DHL")))
    .select(
        "*",
        F.concat("status", F.lit(" "), "carrier").alias("status_carrier"),
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
    )
)

metrics = (
    base
    .filter(F.col("delivered_date") > "2024-01-23")
    .select(
        "id",
        "delivered_date",
        "days_since_delivered_date",
        F.count("days_since_delivered_date").over(w).alias("last7_count_days_since_delivered_date"),
        F.avg("days_since_delivered_date").over(w).alias("last7_avg_days_since_delivered_date"),
        F.max("days_since_delivered_date").over(w).alias("last7_max_days_since_delivered_date"),
        F.min("days_since_delivered_date").over(w).alias("last7_min_days_since_delivered_date"),
        F.min("days_since_delivered_date").over(w2).alias("order_id_min_days_since_delivered_date"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.ntile(10).over(w3).alias("last7_min_days_since_delivered_date_bucket"),
        F.sum("last7_count_days_since_delivered_date").over(w4).alias("running_last7_count_days_since_delivered_date"),
    )
)

valid = (
    with_windows
    .filter(F.col("order_id_min_days_since_delivered_date") == 43)
    .select(
        "id",
        "last7_avg_days_since_delivered_date",
        "last7_max_days_since_delivered_date",
        "last7_min_days_since_delivered_date",
        "last7_min_days_since_delivered_date_bucket",
        "running_last7_count_days_since_delivered_date",
        F.least("last7_min_days_since_delivered_date", "last7_count_days_since_delivered_date").alias("least_last7_min_days_since_delivered_date_last7_count_days_since_delivered_date"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("last7_max_days_since_delivered_date") >= 61, "LARGE").otherwise("SMALL").alias("last7_max_days_since_delivered_date_tier"),
        F.when(F.col("last7_min_days_since_delivered_date") >= 82, "LARGE").otherwise("SMALL").alias("last7_min_days_since_delivered_date_tier"),
    )
    .agg(
        F.round(F.avg("last7_min_days_since_delivered_date"), 2).alias("avg_last7_min_days_since_delivered_date"),
        F.count("*").alias("row_count"),
        F.percentile_approx("last7_min_days_since_delivered_date", 0.95).alias("last7_min_days_since_delivered_date_p95"),
    )
    .withColumn("avg_last7_min_days_since_delivered_date_rank", F.rank().over(w5))
)
