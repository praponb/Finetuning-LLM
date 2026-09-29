from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("amount") < 780)

order_id_summary = (
    filtered_payments
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
        F.max("amount").alias("max_amount"),
    )
)

result = order_id_summary
