from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 1770, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
        F.countDistinct("status").alias("distinct_status"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .orderBy(F.col("distinct_status").desc_nulls_last())
)
