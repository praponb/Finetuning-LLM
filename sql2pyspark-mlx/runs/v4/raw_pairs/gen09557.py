from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4400, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.round(F.avg("amount"), 2).alias("avg_amount_2"),
    )
    .filter(F.col("avg_amount_2") >= 0)
)
