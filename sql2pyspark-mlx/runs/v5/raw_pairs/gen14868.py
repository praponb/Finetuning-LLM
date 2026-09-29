from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 240)
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        F.round((F.col("monthly_fee") * 1.07), 2).alias("monthly_fee_scaled"),
    )
)
