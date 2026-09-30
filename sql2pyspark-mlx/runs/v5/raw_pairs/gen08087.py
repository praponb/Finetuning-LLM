from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .rollup(
        F.when(F.col("burst_id") >= 8, "HIGH").otherwise("LOW").alias("burst_id_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("prev_paid_at")) - F.unix_timestamp(F.min("prev_paid_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
        ((F.unix_timestamp(F.max("prev_paid_at")) - F.unix_timestamp(F.min("prev_paid_at"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
    .orderBy(F.col("card_count").desc())
)
