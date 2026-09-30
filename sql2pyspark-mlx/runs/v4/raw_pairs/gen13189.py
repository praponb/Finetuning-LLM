from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")

latest = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .groupBy(
        F.when(F.col("monthly_fee") >= 258, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
        "customer_id",
        F.date_trunc("WEEK", "end_date").alias("week_start"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("monthly_fee_per_basic"),
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
    )
)
