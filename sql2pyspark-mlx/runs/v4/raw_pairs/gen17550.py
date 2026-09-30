from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-04-18").cast("date"))
    .withColumn("clean_plan", F.trim("plan"))
)

result = (
    valid
    .select("id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("monthly_fee").between(100, 170))
        .select("id")
    )
)
