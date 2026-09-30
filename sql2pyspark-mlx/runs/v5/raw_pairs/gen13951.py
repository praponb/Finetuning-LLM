from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([8, 11]) & (F.col("monthly_fee") <= 70))
    .groupBy("plan")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.countDistinct("monthly_fee").alias("unique_plans"),
    )
    .orderBy("plan")
)
