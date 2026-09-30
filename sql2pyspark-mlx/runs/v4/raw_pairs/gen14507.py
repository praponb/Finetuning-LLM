from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "status")
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
