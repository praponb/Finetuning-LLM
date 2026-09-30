from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "ACTIVE") | (F.col("customer_id") == 38))
    .select(
        "id",
        "monthly_fee",
        "start_date",
        F.initcap(F.trim("status")).alias("status_title"),
        F.upper("plan").alias("plan_upper"),
    )
    .orderBy("start_date")
)
