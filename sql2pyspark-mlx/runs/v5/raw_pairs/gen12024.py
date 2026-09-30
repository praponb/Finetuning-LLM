from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("txn_time") > F.to_date(F.lit("2023-07-06")))
    .select("*", F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"))
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("QUARTER", "txn_time").alias("quarter_start"),
        F.when(F.col("amount") >= 4200, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "7-Eleven", 1)), F.lit(0)), 2).alias("amount_per_7_eleven"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
    )
)
