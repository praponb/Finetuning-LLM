from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-06-23").cast("date"))
    .withColumn("plan_length", F.length("plan"))
)

result = (
    base
    .select("id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("start_date") >= "2025-10-16")
        .select("id")
    )
)
