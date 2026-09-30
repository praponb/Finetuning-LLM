from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "bank_transfer"]) & (F.col("paid_at") >= "2024-07-01") & F.col("order_id").isin([3, 6, 16]))
    .groupBy(
        F.when(F.col("amount") >= 1220, "high").when(F.col("amount") >= 380, "mid").otherwise("low").alias("amount_bucket"),
        "status",
    )
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.max("amount").alias("max_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy(F.col("amount_bucket").desc())
)
