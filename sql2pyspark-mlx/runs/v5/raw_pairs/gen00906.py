from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "start_date",
        "monthly_fee",
        F.when(F.col("plan") == "premium", 1).otherwise(0).alias("is_premium"),
    )
    .orderBy("monthly_fee", "start_date")
)
