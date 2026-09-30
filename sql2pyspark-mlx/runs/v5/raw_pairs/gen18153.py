from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2023-05-06"))) & F.col("customer_id").isNotNull())
    .withColumn("status_length", F.length("status"))
)

result = (
    filtered
    .select("status", "plan")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date") <= "2025-09-01")
        .select("status", "plan")
    )
)
