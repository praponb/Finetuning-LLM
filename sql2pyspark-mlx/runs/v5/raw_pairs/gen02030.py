from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1380, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.count_if(F.col("method") == "card").alias("card_events"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("amount_per_success"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("amount_stddev").desc())
)
