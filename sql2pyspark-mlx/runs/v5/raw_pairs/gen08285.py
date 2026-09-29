from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(10, 16))
    .groupBy(
        F.when(F.col("amount") >= 3300, "high").when(F.col("amount") >= 1400, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.countDistinct("amount").alias("unique_merchants"),
    )
    .filter(F.col("unique_merchants") > 4)
    .select("amount_bucket", "total_amount", "unique_merchants")
    .orderBy("amount_bucket")
)
