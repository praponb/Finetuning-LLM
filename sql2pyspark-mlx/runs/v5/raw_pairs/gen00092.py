from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", F.year("txn_time").alias("txn_year"))
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
