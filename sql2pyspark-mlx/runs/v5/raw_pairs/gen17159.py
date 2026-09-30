from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "basic"]))
    .select(
        "id",
        "customer_id",
        "end_date",
        "start_date",
        F.trim("plan").alias("clean_plan"),
    )
)
