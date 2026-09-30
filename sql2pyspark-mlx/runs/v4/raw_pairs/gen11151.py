from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(F.col("paid_at") > F.to_timestamp(F.lit("2023-06-25 15:00:00")))
    .withColumn("method_group", F.when(F.col("method").isin(["card", "promptpay"]), "Core").otherwise("Other"))
)

result = (
    filtered
    .groupBy(
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
        "status",
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
    )
)
