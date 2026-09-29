from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("plan") == "premium") | F.col("status").isin(["CANCELLED", "EXPIRED"]))
)

customer_id_summary = (
    base
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)

result = (
    customer_id_summary
    .filter(F.col("cancelled_count") < 5)
    .select("customer_id", "cancelled_count", "monthly_fee_count")
    .orderBy(F.col("customer_id").desc())
)
