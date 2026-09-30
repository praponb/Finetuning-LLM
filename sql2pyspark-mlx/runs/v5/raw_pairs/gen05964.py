from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("plan").isin(["basic", "enterprise"]), "priority").otherwise("other").alias("plan_group"),
    )
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .filter(F.col("max_monthly_fee") > 150)
)
