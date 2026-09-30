from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount_per_refund").desc())

result = (
    df_transactions
    .groupBy("txn_type", F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "REFUND", 1)), F.lit(0)), 2).alias("amount_per_refund"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("amount_per_refund_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
