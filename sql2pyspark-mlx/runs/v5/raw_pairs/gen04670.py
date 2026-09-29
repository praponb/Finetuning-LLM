from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(F.col("paid_at") >= "2024-09-10")
    .withColumn("amount_whole", F.floor("amount"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("UNKNOWN")))
)

result = (
    filtered
    .withColumn("amount_tier", F.when(F.col("amount") >= 1490, "PREMIUM").otherwise("STANDARD"))
    .withColumn("amount_whole_tier", F.when(F.col("amount_whole") >= 63, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["amount_tier", "amount_whole_tier"], ["amount_tier"], []],
        "amount_tier",
        "amount_whole_tier",
    )
    .agg(
        F.round(F.sum("amount_whole") / F.nullif(F.count(F.when(F.col("method") == "paypal", 1)), F.lit(0)), 2).alias("amount_whole_per_paypal"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount")).otherwise(0)).alias("promptpay_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("promptpay_amount")
)
