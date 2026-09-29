from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1350, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") > 1270)
)
