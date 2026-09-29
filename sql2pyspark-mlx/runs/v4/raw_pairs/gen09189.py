from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("plan").isin(["basic", "enterprise"]), "group_a").otherwise("other").alias("plan_group"),
    )
    .agg(F.count("*").alias("subscription_count"))
    .orderBy(F.col("plan_group").desc())
    .limit(5)
)
