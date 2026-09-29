from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "monthly_fee",
        "status",
        F.upper("status").alias("status_upper"),
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)
