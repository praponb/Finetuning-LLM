from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("running_count_status_length").desc())
w3 = Window.partitionBy("order_id").orderBy(F.col("running_sum_status_length_p95").desc())

filtered = (
    df_payments
    .filter(F.col("method").isNotNull() & (~F.col("method").eqNullSafe(F.lit("card"))))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.length("status").alias("status_length"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

metrics = (
    filtered
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status_length",
        F.count("status_length").over(w).alias("running_count_status_length"),
        F.avg("status_length").over(w).alias("running_avg_status_length"),
        F.sum("status_length").over(w).alias("running_sum_status_length"),
    )
)

cleaned = (
    metrics
    .filter(F.col("paid_at") > F.lit("2023-09-15").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status_length",
        "running_count_status_length",
        "running_sum_status_length",
        F.when(F.col("method").isin(["cash", "bank_transfer"]), "Tier 1").otherwise("Other").alias("method_group"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.status"))
)

ranked = (
    with_dims
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .groupBy("order_id", "status", F.date_trunc("year", "paid_at").alias("year_start"))
    .agg(
        F.min("paid_at").alias("started_at"),
        F.max("paid_at").alias("last_paid_at"),
        F.percentile_approx("running_sum_status_length", 0.95).alias("running_sum_status_length_p95"),
        F.round(F.avg("status_length"), 2).alias("avg_status_length"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .withColumn("running_sum_status_length_p95_rank", F.rank().over(w3))
)
