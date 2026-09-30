from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("amount_tier", F.when(F.col("amount") >= 1230, "HIGH").otherwise("LOW"))
    .cube("amount_tier", "status")
    .agg(
        F.count_if(F.col("method") == "paypal").alias("paypal_events"),
        F.avg(F.when(F.col("method") == "promptpay", F.col("amount"))).alias("promptpay_avg_amount"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.grouping("amount_tier").alias("grouping_id"),
    )
    .filter(F.col("promptpay_avg_amount") >= 0)
    .orderBy("amount_stddev")
)
