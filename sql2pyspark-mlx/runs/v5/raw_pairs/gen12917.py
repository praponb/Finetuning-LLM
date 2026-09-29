from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        F.when(F.col("monthly_fee") >= 233, "High").when(F.col("monthly_fee") >= 54, "Medium").otherwise("Low").alias("monthly_fee_level"),
        F.date_sub("start_date", 18).alias("start_date_minus_18d"),
    )
    .orderBy(F.col("id").desc(), "monthly_fee_level")
)
