from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.max("amount").alias("max_amount"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("num_txn_types").desc_nulls_first())
)
