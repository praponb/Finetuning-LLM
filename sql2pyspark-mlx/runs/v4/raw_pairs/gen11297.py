from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        "plan",
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)
