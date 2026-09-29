from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("order_id").isin([6, 22, 27]) & F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]))
)

summary = (
    recent_payments
    .groupBy("method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    summary
    .select("method", "status_values")
    .orderBy(F.col("status_values").desc(), "method")
)
