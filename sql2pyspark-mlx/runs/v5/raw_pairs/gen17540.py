from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.partitionBy("status", "order_id").orderBy("cost", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("cost_bucket").desc())
w_recent = Window.partitionBy("order_id").orderBy("cost_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-2, -1)
w_part = Window.partitionBy("order_id").orderBy("last_known_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy(F.col("cost_bucket_percent_rank").desc_nulls_first())

with_windows = (
    df_shipments
    .filter(F.col("cost") <= 67)
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.ntile(10).over(w2).alias("cost_bucket"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

cleaned = (
    ranked
    .filter((F.col("shipped_date") > F.lit("2023-03-16").cast("date")) & (F.col("cost") > 26))
    .withColumn("days_since_shipped_date", F.datediff(F.current_date(), "shipped_date"))
)

metrics = (
    cleaned
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost_share",
        "cost_bucket",
        "days_since_shipped_date",
        F.percent_rank().over(w4).alias("cost_bucket_percent_rank"),
        F.last("cost", ignorenulls=True).over(w_recent).alias("last_known_cost"),
        F.avg("cost_bucket").over(w6).alias("cost_bucket_avg_2_prior"),
    )
)

metrics_shipments = (
    metrics
    .select(
        "*",
        F.sum("days_since_shipped_date").over(w_part).alias("running_sum_days_since_shipped_date"),
        F.count("days_since_shipped_date").over(w_part).alias("running_count_days_since_shipped_date"),
        F.avg("days_since_shipped_date").over(w_part).alias("running_avg_days_since_shipped_date"),
        F.min("days_since_shipped_date").over(w_part).alias("running_min_days_since_shipped_date"),
        F.percent_rank().over(w8).alias("cost_bucket_percent_rank_percent_rank"),
    )
)

result = (
    metrics_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.channel").alias("channel"), F.col("p.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("e.cost_bucket_avg_2_prior")), 2).alias("avg_cost_bucket_avg_2_prior"),
        F.round(F.avg(F.col("e.running_count_days_since_shipped_date")), 2).alias("avg_running_count_days_since_shipped_date"),
    )
    .orderBy("status")
)
