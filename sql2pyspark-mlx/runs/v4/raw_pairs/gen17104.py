from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("amount_tier", F.when(F.col("amount") >= 1620, "HIGH").otherwise("LOW"))
    .withColumn("year_start", F.date_trunc("year", "paid_at"))
    .groupingSets(
        [["amount_tier", "year_start"], ["amount_tier"], []],
        "amount_tier",
        "year_start",
    )
    .agg(
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.sum(F.when(F.col("method") == "paypal", F.col("amount")).otherwise(0)).alias("paypal_amount"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.min("amount").alias("min_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("min_amount") >= 0)
    .orderBy(F.col("year_start").desc())
)
