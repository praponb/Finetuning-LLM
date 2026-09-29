from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .cube("order_id")
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("amount_per_failed"),
        F.sum(F.when(F.col("method") == "card", F.col("burst_id")).otherwise(0)).alias("card_burst_id"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("status") == "REFUNDED", 1)), F.lit(0)), 2).alias("burst_id_per_refunded"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("order_id").asc_nulls_last())
)
