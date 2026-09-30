from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.orderBy("amount", "id")
w_part = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("status", "order_id").orderBy("amount", "id")
w5 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

scored = (
    df_payments
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("next_amount", F.lead("amount", 2).over(w2))
    .withColumn("amount_avg_6_prior", F.avg("amount").over(w_part))
    .withColumn("prev_amount", F.lag("amount", 2).over(w4))
    .withColumn("amount_count_30d", F.count("amount").over(w5))
)

result = (
    scored
    .rollup("method")
    .agg(
        F.round(F.avg("next_amount"), 2).alias("avg_next_amount"),
        F.percentile_approx("prev_amount", [0.25, 0.5, 0.75]).alias("prev_amount_quartiles"),
        F.sum(F.when(F.col("method") == "cash", F.col("prev_amount")).otherwise(0)).alias("cash_prev_amount"),
        F.round(F.sum("prev_amount") / F.nullif(F.count(F.when(F.col("status") == "REFUNDED", 1)), F.lit(0)), 2).alias("prev_amount_per_refunded"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount_avg_6_prior"))).alias("failed_total_amount_avg_6_prior"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.grouping("method").alias("grouping_level"),
    )
)
