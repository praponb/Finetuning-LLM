from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 120)
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        F.floor("monthly_fee").alias("monthly_fee_whole"),
    )
)

result = (
    base
    .select("status", "customer_id")
    .exceptAll(
        df_subscriptions
        .filter(~F.col("plan").isin(["enterprise", "basic", "premium"]))
        .select("status", "customer_id")
    )
)
