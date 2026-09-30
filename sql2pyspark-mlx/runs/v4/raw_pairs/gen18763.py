from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("method").isNotNull())
    .withColumn("paid_at_hour", F.hour("paid_at"))
    .withColumn("method_clean", F.coalesce("method", F.lit("N/A")))
)

prev_events = filtered.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL"))
    .rollup("is_new_session_tier")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.round(F.sum("paid_at_hour") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("paid_at_hour_per_success"),
        F.percentile_approx("paid_at_hour", 0.5).alias("paid_at_hour_p50"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
        F.grouping("is_new_session_tier").alias("gid"),
    )
    .orderBy(F.col("is_new_session_tier").desc_nulls_last())
)
