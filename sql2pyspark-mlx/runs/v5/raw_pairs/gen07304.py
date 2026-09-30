from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["CANCELLED", "PAUSED"]) & (F.year("end_date") >= 2024))
    .select(
        "id",
        "monthly_fee",
        "status",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
    .orderBy(F.col("id").desc(), "monthly_fee_filled")
)
