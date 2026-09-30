from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_first())
w4 = Window.partitionBy("txn_type")

scored = (
    df_transactions
    .withColumn("first_known_merchant", F.first("merchant", ignorenulls=True).over(w))
    .withColumn("amount_sum_90d", F.sum("amount").over(w2))
    .withColumn("amount_dense_rank", F.dense_rank().over(w3))
    .withColumn("txn_type_stddev_samp_amount", F.stddev_samp("amount").over(w4))
)

result = (
    scored
    .groupBy(
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
        F.when(F.col("amount_dense_rank") >= 6, "LARGE").otherwise("SMALL").alias("amount_dense_rank_tier"),
    )
    .agg(
        F.avg(F.when(F.col("merchant") == "Shell", F.col("amount_sum_90d"))).alias("shell_avg_amount_sum_90d"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.avg(F.when(F.col("merchant") == "Lazada", F.col("amount_sum_90d"))).alias("lazada_avg_amount_sum_90d"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("txn_type_stddev_samp_amount")).otherwise(0)).alias("debit_txn_type_stddev_samp_amount"),
    )
    .filter(F.col("debit_count") > 1)
    .orderBy(F.col("year_start").desc_nulls_last())
)
