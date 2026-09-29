from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("status").desc_nulls_first())
)
