from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.country").alias("country"), F.col("e.plan").alias("plan"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.monthly_fee")).alias("max_monthly_fee"),
    )
    .orderBy(F.col("max_monthly_fee").desc())
)
