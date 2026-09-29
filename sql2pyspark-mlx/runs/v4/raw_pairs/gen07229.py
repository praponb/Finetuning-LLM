from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "REFUNDED") & (F.col("order_id") == 15))
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
        F.max("amount").alias("max_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
