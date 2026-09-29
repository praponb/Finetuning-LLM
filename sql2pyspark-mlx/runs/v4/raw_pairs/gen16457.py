from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "Lazada", 1)), F.lit(0)), 2).alias("amount_per_lazada"),
    )
    .orderBy(F.col("txn_type").desc_nulls_last())
)
