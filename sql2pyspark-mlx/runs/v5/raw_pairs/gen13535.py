from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        F.col("signup_date").alias("event_date"),
        F.lit("customer").alias("record_type"),
    )
    .unionByName(
        df_subscriptions
        .filter(F.col("plan").isin(["basic", "standard"]))
        .select(
        F.col("end_date").alias("event_date"),
        F.lit("subscription").alias("record_type"),
    )
    )
    .orderBy("event_date")
)
