from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "monthly_fee",
        "status",
        F.concat_ws(",", "status", "plan").alias("status_plan_key"),
    )
    .orderBy(F.col("id").desc(), "monthly_fee")
)
