from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").asc_nulls_last(), "id")

deduped = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("monthly_fee") >= 241, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("monthly_fee_per_enterprise"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_count").desc())
)
