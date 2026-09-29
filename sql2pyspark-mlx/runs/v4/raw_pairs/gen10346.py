from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 120) | (F.col("customer_id") == 20))
    .select(
        "id",
        "status",
        "plan",
        F.when(F.col("plan").isin(["enterprise", "standard"]), "Priority").otherwise("Other").alias("plan_group"),
    )
)
