from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 180)
    .select(
        "id",
        "plan",
        "monthly_fee",
        F.date_sub("start_date", 68).alias("start_date_minus_68d"),
        F.when(F.col("monthly_fee") >= 230, "High").when(F.col("monthly_fee") >= 66, "Medium").otherwise("Low").alias("monthly_fee_band"),
    )
    .orderBy("start_date_minus_68d", F.col("id").desc())
)
