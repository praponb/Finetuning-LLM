from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("customer_id") == 3) & F.col("plan").isin(["standard", "basic", "premium"]) & (F.col("status") == "EXPIRED"))
)

customer_id_summary = (
    base
    .groupBy("customer_id")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
)

result = (
    customer_id_summary
    .filter(F.col("max_monthly_fee") < 160)
    .select("customer_id", "total_monthly_fee")
    .orderBy(F.col("customer_id").desc(), F.col("total_monthly_fee").desc())
)
