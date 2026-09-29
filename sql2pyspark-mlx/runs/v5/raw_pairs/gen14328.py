from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_trunc("QUARTER", "paid_at").alias("quarter_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
    )
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
