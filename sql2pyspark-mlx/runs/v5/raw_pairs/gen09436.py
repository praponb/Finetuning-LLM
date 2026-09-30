from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "monthly_fee",
        "start_date",
        F.when(F.col("status") == "ACTIVE", 1).otherwise(0).alias("is_active"),
    )
    .orderBy("start_date")
)
