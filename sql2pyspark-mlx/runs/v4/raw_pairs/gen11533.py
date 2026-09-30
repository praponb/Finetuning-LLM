from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        "plan",
        F.concat("status", F.lit("/"), "plan").alias("status_plan"),
    )
    .orderBy("plan")
)
