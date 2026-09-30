from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isin([3, 8, 36, 39]) | F.hour("paid_at").between(11, 16))
)

summary = (
    base
    .groupBy("status")
    .agg(
        F.count("*").alias("num_payments"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    summary
    .filter(F.col("num_payments") > 4)
    .select("status", "num_payments", "status_values")
    .orderBy("status_values", F.col("status").desc())
)
