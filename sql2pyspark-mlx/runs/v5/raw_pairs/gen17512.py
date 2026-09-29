from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([4, 18]))
    .select(
        "end_date",
        "start_date",
        F.when(F.col("status").isin(["CANCELLED", "EXPIRED"]), "Core").otherwise("Other").alias("status_group"),
        F.lower("plan").alias("plan_lower"),
    )
    .orderBy("status_group")
)
