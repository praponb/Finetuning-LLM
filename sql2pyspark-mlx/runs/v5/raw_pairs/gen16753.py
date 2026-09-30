from pyspark.sql import functions as F

valid = (
    df_payments
    .filter((F.col("method") == "promptpay") & (F.col("paid_at") > F.lit("2023-11-04 03:00:00").cast("timestamp")))
    .select(
        "*",
        F.when(F.col("amount") >= 1270, "High").when(F.col("amount") >= 450, "Medium").otherwise("Low").alias("amount_tier"),
    )
)

result = (
    valid
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1380, "PREMIUM").otherwise("STANDARD"))
    .rollup("year_start", "amount_tier")
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes_2"),
        F.sum(F.when(F.col("method") == "card", F.col("amount")).otherwise(0)).alias("card_amount"),
        F.sum(F.when(F.col("method") == "card", F.col("amount"))).alias("card_total_amount"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.grouping("year_start").alias("grouping_level"),
    )
    .filter(F.col("card_amount") > 0)
)
