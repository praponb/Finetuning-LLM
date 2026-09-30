from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1750, "HIGH").otherwise("LOW").alias("amount_tier"),
        "order_id",
    )
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.countDistinct("status").alias("distinct_status"),
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.size(F.collect_set("method")).alias("num_methods"),
    )
)
