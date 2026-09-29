from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", F.year("start_date").alias("start_year"))
    .agg(
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
    )
)
