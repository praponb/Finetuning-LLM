from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 7)
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
        F.avg("amount").alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .filter(F.col("avg_amount") < 990)
    .orderBy(F.col("method_values").desc())
)
