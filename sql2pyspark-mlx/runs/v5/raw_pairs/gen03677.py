from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("week", "txn_time").alias("week_start"), "merchant")
    .agg(
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
    )
    .orderBy("num_merchants")
)
