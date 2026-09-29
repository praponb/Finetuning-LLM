from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1750, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("method") == "paypal", 1)), F.lit(0)), 2).alias("amount_per_paypal"),
        F.max(F.when(F.col("method") == "cash", F.col("amount"))).alias("cash_max_amount"),
    )
    .orderBy(F.col("week_start").desc_nulls_first())
)
