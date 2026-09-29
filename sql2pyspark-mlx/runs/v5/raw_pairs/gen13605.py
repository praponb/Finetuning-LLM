from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "status",
        "end_date",
        F.when(F.col("plan") == "premium", 1).otherwise(0).alias("is_premium"),
    )
)
