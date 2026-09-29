from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 263, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count_if(F.col("plan") == "standard").alias("standard_events"),
    )
    .orderBy(F.col("monthly_fee_tier").desc())
)
