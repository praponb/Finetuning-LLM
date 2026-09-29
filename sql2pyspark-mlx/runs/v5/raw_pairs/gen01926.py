from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "start_date",
        "customer_id",
        "plan",
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)
