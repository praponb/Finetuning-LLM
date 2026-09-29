from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_trunc("WEEK", "end_date").alias("week_start"))
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .orderBy(F.col("week_start").desc_nulls_last())
)
