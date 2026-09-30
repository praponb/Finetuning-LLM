from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("WEEK", "txn_time").alias("week_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max(F.when(F.col("merchant") == "Lazada", F.col("amount"))).alias("lazada_max_amount"),
        F.max("amount").alias("max_amount"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
    )
    .orderBy(F.col("lazada_max_amount").asc_nulls_last())
)
