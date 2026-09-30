from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") >= 8)
    .orderBy("method_values")
)
