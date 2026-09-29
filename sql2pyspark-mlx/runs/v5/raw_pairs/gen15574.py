from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 58000, "LARGE").otherwise("SMALL").alias("balance_tier"),
        "customer_id",
    )
    .agg(
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_rows"),
        F.stddev_samp("balance").alias("balance_stddev_2"),
    )
    .orderBy(F.col("balance_stddev").asc_nulls_last())
)
