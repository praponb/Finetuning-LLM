from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "status",
        F.when(F.col("monthly_fee") >= 250, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "standard").alias("standard_events"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
    .filter(F.col("standard_events") >= 1)
    .orderBy(F.col("avg_monthly_fee").desc_nulls_first())
)
