from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3600, "HIGH").otherwise("LOW").alias("amount_tier"),
        "account_id",
    )
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.min("txn_time").alias("first_seen"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
    )
    .filter(F.col("span_minutes") > 0)
    .orderBy(F.col("amount_tier").asc_nulls_last())
)
