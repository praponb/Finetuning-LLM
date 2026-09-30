from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "method")
w3 = Window.partitionBy("status", "method").orderBy(F.col("amount_int").desc())
w4 = Window.partitionBy("status").orderBy("amount_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("status").orderBy("amount_int", "id")

base = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("method").eqNullSafe(F.lit("cash"))))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.last("amount_int", ignorenulls=True).over(w_part).alias("last_known_amount_int"),
        F.avg("amount_int").over(w2).alias("order_id_avg_amount_int"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "paid_at", "status", "amount_int")
)

with_windows = (
    ranked
    .filter(F.col("paid_at") < "2025-09-22")
    .withColumn("running_min_amount_int", F.min("amount_int").over(w4))
    .withColumn("running_count_amount_int", F.count("amount_int").over(w4))
    .withColumn("running_sum_amount_int", F.sum("amount_int").over(w4))
    .withColumn("amount_int_bucket", F.ntile(3).over(w_hist))
)

summary = (
    with_windows
    .cube("order_id")
    .agg(
        F.percentile_approx("running_count_amount_int", [0.25, 0.5, 0.75]).alias("running_count_amount_int_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.countDistinct("status").alias("distinct_status"),
        F.grouping("order_id").alias("grouping_id"),
    )
)

result = (
    summary
    .select("num_status", "refunded_rows")
    .orderBy(F.col("refunded_rows").asc_nulls_first())
)
