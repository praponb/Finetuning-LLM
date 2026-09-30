from pyspark.sql import functions as F

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("amount") <= 640))
    .withColumn("amount_whole", F.floor("amount"))
)

result = (
    valid
    .groupBy("order_id")
    .pivot("status", ["FAILED", "REFUNDED"])
    .agg(F.sum("amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "order_id",
        F.col("FAILED_value_sum").alias("failed_value_sum"),
        F.col("FAILED_cnt").alias("failed_cnt"),
        F.col("REFUNDED_value_sum").alias("refunded_value_sum"),
        F.col("REFUNDED_cnt").alias("refunded_cnt"),
    )
)
