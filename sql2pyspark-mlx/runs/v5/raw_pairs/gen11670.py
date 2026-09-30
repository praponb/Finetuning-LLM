from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("method").isNotNull() & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.concat("status", F.lit("/"), "method").alias("status_method"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "method", "paid_at")
)

with_gaps = (
    deduped
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w2).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w2) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w3))

metrics = (
    islands
    .filter(F.col("session_id") < 7)
    .select(
        "*",
        F.sum("session_id").over(w3).alias("running_sum_session_id"),
        F.max("session_id").over(w3).alias("running_max_session_id"),
        F.avg("session_id").over(w3).alias("running_avg_session_id"),
        F.count("session_id").over(w3).alias("running_count_session_id"),
    )
)

kept_keys = (
    metrics
    .select("id", "method")
    .intersect(
        df_payments
        .filter(F.col("method").isin(["paypal", "bank_transfer", "card"]))
        .select("id", "method")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

totals = (
    verified
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .groupingSets(
        [["activity_month", "session_id"], ["activity_month"]],
        "activity_month",
        "session_id",
    )
    .agg(
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    totals
    .select(
        "activity_month",
        "session_id",
        "session_start_flag_quartiles",
        "row_count",
        "gid",
    )
    .orderBy(F.col("session_id").desc(), "activity_month")
)
