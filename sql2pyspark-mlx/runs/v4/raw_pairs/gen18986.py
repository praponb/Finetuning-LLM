from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc())
w2 = Window.partitionBy("country", "customer_id").orderBy(F.col("start_date").desc_nulls_first(), "id")
w3 = Window.orderBy(F.col("days_since_start_date").desc_nulls_last())
w4 = Window.partitionBy("country").orderBy("days_since_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("days_since_start_date").desc(), "id")
w6 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("segment").orderBy("customer_credit_limit", "id")
w8 = Window.partitionBy("status").orderBy(F.col("days_since_start_date_dense_rank_p50").desc())

joined = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.credit_limit"), F.col("b.segment"), F.col("b.country"))
)

ranked = (
    joined
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

joined_subscriptions = (
    ranked
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.credit_limit").alias("customer_credit_limit"))
)

latest = (
    joined_subscriptions
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

base = (
    latest
    .filter(F.col("plan").isin(["premium", "basic", "enterprise"]))
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "segment",
        "country",
        "customer_credit_limit",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)

metrics = (
    base
    .withColumn("days_since_start_date_dense_rank", F.dense_rank().over(w3))
    .withColumn("first_known_segment", F.first("segment", ignorenulls=True).over(w4))
    .withColumn("days_since_start_date_bucket", F.ntile(3).over(w5))
    .withColumn("last_known_segment", F.last("segment", ignorenulls=True).over(w6))
    .withColumn("prev_days_since_start_date", F.lag("days_since_start_date", 2).over(w_recent))
)

stats = (
    metrics
    .groupBy(
        "status",
        F.date_trunc("WEEK", "end_date").alias("week_start"),
        F.when(F.col("days_since_start_date_bucket") >= 7, "HIGH").otherwise("LOW").alias("days_since_start_date_bucket_tier"),
    )
    .agg(
        F.round(F.avg("days_since_start_date_dense_rank"), 2).alias("avg_days_since_start_date_dense_rank"),
        F.percentile_approx("days_since_start_date_dense_rank", 0.5).alias("days_since_start_date_dense_rank_p50"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_rows"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("prev_days_since_start_date")).otherwise(0)).alias("expired_prev_days_since_start_date"),
    )
    .withColumn("days_since_start_date_dense_rank_p50_percent_rank", F.percent_rank().over(w8))
)

result = (
    stats
    .select(
        "status",
        "week_start",
        "days_since_start_date_bucket_tier",
        "avg_days_since_start_date_dense_rank",
        "days_since_start_date_dense_rank_p50",
        "expired_prev_days_since_start_date",
    )
    .orderBy("days_since_start_date_dense_rank_p50")
)
