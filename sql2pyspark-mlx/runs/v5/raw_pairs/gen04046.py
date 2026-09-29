from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
)
