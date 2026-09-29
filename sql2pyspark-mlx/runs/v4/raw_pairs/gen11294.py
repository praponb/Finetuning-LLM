from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 30) & (F.to_date("paid_at") < "2025-04-01"))
    .cube("method", "status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("method")
)
