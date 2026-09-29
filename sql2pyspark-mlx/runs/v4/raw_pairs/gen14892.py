from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3700, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "Lazada", 1)), F.lit(0)), 2).alias("amount_per_lazada"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .filter(F.col("amount_stddev") > 0)
    .orderBy(F.col("amount_tier").asc_nulls_last())
)
