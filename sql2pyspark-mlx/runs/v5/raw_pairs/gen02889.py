from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.date_trunc("QUARTER", "start_date").alias("quarter_start"),
        F.when(F.col("monthly_fee") >= 186, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "standard", 1)), F.lit(0)), 2).alias("monthly_fee_per_standard"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_rows"),
    )
)
