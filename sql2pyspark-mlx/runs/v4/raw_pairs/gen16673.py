from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2023-09-30")
    .groupBy(
        "status",
        "method",
        F.when(F.col("method").isin(["promptpay", "paypal"]), "tier_1").otherwise("other").alias("method_group"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"),
    )
    .filter(F.col("cash_count") >= 53)
    .drop("cash_count")
    .orderBy("method_group")
)
