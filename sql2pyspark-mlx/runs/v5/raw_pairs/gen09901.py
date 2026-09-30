from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2024-10-01")
    .select("id", "start_date", "end_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee").between(90, 240))
        .select("id", "start_date", "end_date")
    )
    .distinct()
    .orderBy("id")
)
