from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("channel").orderBy("payment_paid_at", "id")
w3 = Window.partitionBy("channel").orderBy("payment_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id", "channel").orderBy("session_no", "id")
w5 = Window.partitionBy("quarter_start").orderBy(F.col("next_session_no_p95").desc())

base = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-05-20").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("channel") == "mobile", 1).otherwise(0).alias("is_mobile"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.is_mobile",
        F.col("c.amount").alias("payment_amount"),
        F.col("c.paid_at").alias("payment_paid_at"),
    )
)

with_gaps = (
    with_last
    .withColumn("is_new_burst", F.when(F.col("payment_paid_at") - F.lag("payment_paid_at").over(w2) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "is_mobile",
        "payment_paid_at",
        F.sum("is_new_burst").over(w3).alias("session_no"),
    )
)

metrics = (
    islands
    .withColumn("prev_amount", F.lag("amount").over(w4))
    .withColumn("next_session_no", F.lead("session_no").over(w4))
    .withColumn("event_seq", F.row_number().over(w4))
)

remaining_keys = (
    metrics
    .select("id", "channel")
    .subtract(
        df_orders
        .filter(F.col("customer_id").isin([2, 22, 23, 39]))
        .select("id", "channel")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

totals = (
    verified
    .groupBy(
        F.date_trunc("quarter", "payment_paid_at").alias("quarter_start"),
        F.when(F.col("is_mobile") >= 72, "PREMIUM").otherwise("STANDARD").alias("is_mobile_tier"),
        F.date_trunc("year", "payment_paid_at").alias("year_start"),
    )
    .agg(
        F.percentile_approx("next_session_no", 0.95).alias("next_session_no_p95"),
        F.percentile_approx("session_no", 0.95).alias("session_no_p95"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .withColumn("next_session_no_p95_dense_rank", F.dense_rank().over(w5))
)

result = (
    totals
    .select(
        "quarter_start",
        "is_mobile_tier",
        "year_start",
        "web_rows",
        "distinct_channels",
        "next_session_no_p95_dense_rank",
    )
)
