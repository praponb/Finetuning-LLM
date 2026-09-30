from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2024-05-01"))) & F.col("customer_id").isNotNull())
    .withColumn("end_date_label", F.date_format("end_date", "yyyy-MM"))
)

result = (
    cleaned
    .groupBy(F.date_trunc("year", "end_date").alias("year_start"))
    .agg(
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.percentile_approx("monthly_fee", 0.5).alias("monthly_fee_p50"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
    )
    .filter(F.col("monthly_fee_p50") >= 0)
    .orderBy("expired_count")
)
