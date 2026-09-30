from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3900, "high").when(F.col("amount") >= 1500, "mid").otherwise("low").alias("amount_bucket"),
        F.date_format("txn_time", "yyyy-MM").alias("txn_month"),
    )
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") >= 3800)
    .drop("max_amount")
    .orderBy(F.col("fee_count").desc(), "amount_bucket")
)
