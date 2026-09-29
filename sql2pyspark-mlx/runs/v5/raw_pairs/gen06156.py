from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.when(F.col("balance") >= 84000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.stddev_samp("balance").alias("balance_stddev"),
        F.count_if(F.col("status") == "CLOSED").alias("closed_events"),
        F.count("*").alias("num_rows"),
        F.stddev_samp("balance").alias("balance_stddev_2"),
    )
    .filter(F.col("num_rows") >= 1)
    .orderBy(F.col("num_rows").desc_nulls_first())
)
