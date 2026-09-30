from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") == 2023)
    .select("customer_id", "start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("status") == "CANCELLED")
        .select("customer_id", "start_date")
    )
    .distinct()
    .orderBy(F.col("start_date").desc())
)
