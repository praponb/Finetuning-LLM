from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(840, 1160))
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0)).alias("refunded_count"),
    )
    .filter(F.col("refunded_count") > 72)
    .orderBy("status_values", "status")
    .limit(20)
)
