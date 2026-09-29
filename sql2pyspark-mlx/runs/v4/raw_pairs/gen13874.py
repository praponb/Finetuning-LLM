from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id", "method").orderBy("amount", "id")
w5 = Window.orderBy(F.col("amount").desc())
w6 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("method").orderBy("amount_cume_dist")
w8 = Window.partitionBy("method", "status")
w9 = Window.orderBy(F.col("amount_cume_dist_rank").asc_nulls_first())
w10 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

metrics = (
    df_payments
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("amount_count_7d", F.count("amount").over(w2))
    .withColumn("cumulative_amount", F.sum("amount").over(w3))
    .withColumn("amount_bucket", F.ntile(3).over(w4))
    .withColumn("amount_cume_dist", F.cume_dist().over(w5))
)

remaining_keys = (
    metrics
    .select("id")
    .exceptAll(
        df_payments
        .filter(F.col("method") == "cash")
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics_payments = (
    verified
    .withColumn("amount_cume_dist_avg_7d", F.avg("amount_cume_dist").over(w6))
    .withColumn("amount_cume_dist_rank", F.rank().over(w7))
    .withColumn("amount_count_7d_share", F.round((F.col("amount_count_7d") / F.sum("amount_count_7d").over(w8)), 4))
)

result = (
    metrics_payments
    .select(
        "*",
        F.percent_rank().over(w9).alias("amount_cume_dist_rank_percent_rank"),
        F.avg("amount_cume_dist_avg_7d").over(w10).alias("amount_cume_dist_avg_7d_avg_90d"),
    )
)
