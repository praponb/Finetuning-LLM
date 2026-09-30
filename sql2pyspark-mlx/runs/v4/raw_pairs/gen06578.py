from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipping_fee").asc_nulls_first(), "id")
w2 = Window.orderBy("order_date", "id")
w3 = Window.partitionBy("status")

base = (
    df_shipments
    .filter(F.col("delivered_date") > F.lit("2023-08-30").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
)

with_dims = (
    base
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.shipping_fee"),
        F.col("b.status").alias("order_status"),
        F.col("b.order_date"),
    )
)

deduped = (
    with_dims
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.lead("cost", 2).over(w2).alias("next_cost"),
        F.avg("days_since_shipped_date").over(w3).alias("status_avg_days_since_shipped_date"),
    )
)

result = (
    with_windows
    .withColumn("cost_tier", F.when(F.col("cost") >= 80, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "delivered_date"))
    .groupingSets(
        [["cost_tier", "activity_month"], ["cost_tier"], []],
        "cost_tier",
        "activity_month",
    )
    .agg(
        F.percentile_approx("status_avg_days_since_shipped_date", 0.9).alias("status_avg_days_since_shipped_date_p90"),
        F.round(F.sum("status_avg_days_since_shipped_date") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("status_avg_days_since_shipped_date_per_returned"),
    )
    .filter(F.col("status_avg_days_since_shipped_date_p90") > 0)
    .orderBy(F.col("status_avg_days_since_shipped_date_per_returned").asc_nulls_first())
)
