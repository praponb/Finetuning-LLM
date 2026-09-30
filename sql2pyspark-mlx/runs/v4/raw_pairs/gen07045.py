from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("cash_events").desc())

base = (
    df_payments
    .filter(F.col("paid_at") >= F.to_date(F.lit("2023-11-03")))
    .withColumn("status_lower", F.lower("status"))
)

metrics = (
    base
    .select(
        "*",
        F.last("method", ignorenulls=True).over(w).alias("last_known_method"),
        F.first("method", ignorenulls=True).over(w2).alias("first_known_method"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
    )
)

with_dims = (
    metrics
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.amount").alias("order_amount"), F.col("d.order_date"))
)

result = (
    with_dims
    .groupBy("order_id", "method", "status")
    .agg(
        F.count("*").alias("num_rows"),
        F.max("paid_at").alias("last_seen"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .withColumn("cash_events_dense_rank", F.dense_rank().over(w3))
)
