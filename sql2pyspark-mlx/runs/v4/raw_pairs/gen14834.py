from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2024-02-25 08:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("status_method_key", F.concat_ws(",", "status", "method"))
)

result = (
    base
    .groupBy("method")
    .pivot("status", ["REFUNDED", "SUCCESS"])
    .agg(F.sum("amount").alias("total"))
)
