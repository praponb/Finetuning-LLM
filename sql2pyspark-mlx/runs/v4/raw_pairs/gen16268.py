from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_trunc("year", "end_date").alias("year_start"), "customer_id")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "standard", 1)), F.lit(0)), 2).alias("monthly_fee_per_standard"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .orderBy(F.col("monthly_fee_per_standard").desc())
)
