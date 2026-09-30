from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "account_id",
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.when(F.col("amount") >= 4200, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.when(F.col("txn_type") == "CREDIT", F.col("amount"))).alias("credit_max_amount"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.avg(F.when(F.col("txn_type") == "CREDIT", F.col("amount"))).alias("credit_avg_amount"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
    )
    .orderBy(F.col("duration_min").desc())
)
