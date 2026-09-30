from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method")
w4 = Window.partitionBy("method").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (~F.col("method").eqNullSafe(F.lit("cash"))))
    .withColumn("amount_level", F.when(F.col("amount") >= 1790, "High").when(F.col("amount") >= 520, "Medium").otherwise("Low"))
)

valid = (
    base
    .filter(~F.col("method").eqNullSafe(F.lit("cash")))
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.concat_ws("-", "method", "status").alias("method_status_key"),
    )
)

flagged = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    flagged
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

enriched = (
    sessions_payments
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "burst_id",
        F.count("burst_id").over(w3).alias("method_count_burst_id"),
        F.sum("burst_id").over(w4).alias("cumulative_burst_id"),
    )
)

base_payments = (
    enriched
    .filter(F.col("status") == "FAILED")
    .select(
        "id",
        "method",
        "status",
        "burst_id",
        "method_count_burst_id",
        F.to_date("paid_at").alias("paid_at_date"),
        (F.col("cumulative_burst_id") - F.col("burst_id")).alias("cumulative_burst_id_minus_burst_id"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

result = (
    base_payments
    .groupBy(
        F.when(F.col("cumulative_burst_id_minus_burst_id") >= 70, "LARGE").otherwise("SMALL").alias("cumulative_burst_id_minus_burst_id_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.countDistinct("method").alias("distinct_methods"),
    )
    .orderBy(F.col("distinct_methods").desc())
)
