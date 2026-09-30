from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "ACTIVE") | F.col("plan").isin(["standard", "basic"]))
    .select(
        "start_date",
        F.upper("plan").alias("plan_upper"),
        F.upper("plan").alias("plan_upper_2"),
    )
)
