from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2025-09-01")
    .select(F.lit("subscription").alias("record_type"), "start_date")
    .unionByName(
        df_customers
        .filter(F.col("city").isin(["Berlin", "Tokyo"]))
        .select(
        F.lit("customer").alias("record_type"),
        F.col("signup_date").alias("start_date"),
    )
    )
    .distinct()
    .orderBy("record_type")
)
