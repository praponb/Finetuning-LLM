from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
    )
    .filter(F.col("refund_amount") < 1000000)
    .orderBy(F.col("total_amount").asc_nulls_first())
)
