from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "merchant",
        "account_id",
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
    )
    .agg(
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "7-Eleven", 1)), F.lit(0)), 2).alias("amount_per_7_eleven"),
    )
    .filter(F.col("event_count") >= 1)
    .orderBy(F.col("event_count").desc())
)
