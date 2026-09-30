from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
    )
    .orderBy("status_values", "status")
    .limit(50)
)
