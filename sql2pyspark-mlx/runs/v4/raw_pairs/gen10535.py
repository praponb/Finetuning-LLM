from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]) & (F.col("amount") >= 1040))
    .groupBy("status", "method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("status", "status_values", "method")
    .limit(20)
)
