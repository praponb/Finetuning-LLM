from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .select(
        "plan",
        "start_date",
        "customer_id",
        F.concat("status", F.lit(" - "), "plan").alias("status_plan"),
    )
)
