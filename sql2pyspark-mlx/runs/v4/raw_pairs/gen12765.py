from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")
w_recent = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")
w4 = Window.partitionBy("carrier").orderBy(F.col("next_cost").desc_nulls_first())
w5 = Window.partitionBy("carrier").orderBy("next_cost", "id")
w6 = Window.partitionBy("carrier", "status").orderBy("order_order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("channel").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.amount"), F.col("o.channel"), F.col("o.order_date"))
)

with_windows = (
    with_dims
    .withColumn("next_cost", F.lead("cost", 1).over(w))
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w_recent))
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest = (
    deduped
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

with_dims_shipments = (
    latest
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.order_date").alias("order_order_date"))
)

with_windows_shipments = (
    with_dims_shipments
    .select(
        "*",
        F.row_number().over(w5).alias("event_seq"),
        F.sum("next_cost").over(w6).alias("running_next_cost"),
        F.first("next_cost", ignorenulls=True).over(w7).alias("first_known_next_cost"),
    )
)

daily = (
    with_windows_shipments
    .groupBy(F.date_trunc("month", "shipped_date").alias("activity_month"))
    .agg(
        F.percentile_approx("next_cost", [0.25, 0.5, 0.75]).alias("next_cost_quartiles"),
        F.round(F.avg("first_known_next_cost"), 2).alias("avg_first_known_next_cost"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_rows"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("status") == "LOST", F.col("cost"))).alias("lost_total_cost"),
    )
)

result = (
    daily
    .select(
        "activity_month",
        "next_cost_quartiles",
        "delivered_rows",
        "row_count",
        "lost_total_cost",
    )
    .orderBy(F.col("row_count").desc_nulls_last(), "activity_month")
)
