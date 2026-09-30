from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1440, "high").when(F.col("amount") >= 660, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.min("amount").alias("min_amount"),
        F.sum("amount").alias("total_amount"),
    )
)
