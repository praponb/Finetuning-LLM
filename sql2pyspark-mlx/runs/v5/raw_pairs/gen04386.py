from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.upper("status").alias("status_upper"))
)

result = (
    base
    .select("customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("status").isin(["EXPIRED", "ACTIVE", "CANCELLED"]))
        .select("customer_id")
    )
)
