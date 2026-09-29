from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "cash") & F.col("amount").between(710, 720))
    .groupBy(
        F.when(F.col("amount") >= 1500, "high").when(F.col("amount") >= 260, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .filter(F.col("failed_amount") > 27)
)
