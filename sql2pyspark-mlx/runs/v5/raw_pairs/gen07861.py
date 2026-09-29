from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-06-01", "2025-07-01"))
    .select(
        "id",
        "end_date",
        "monthly_fee",
        F.round("monthly_fee", 2).alias("monthly_fee_rounded"),
    )
    .orderBy("monthly_fee")
)
