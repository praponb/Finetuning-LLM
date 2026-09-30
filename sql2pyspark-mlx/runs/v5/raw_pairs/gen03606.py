from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "customer_id",
        "start_date",
        F.date_add("end_date", 75).alias("end_date_plus_75d"),
        F.when(F.col("monthly_fee") >= 247, "High").when(F.col("monthly_fee") >= 95, "Medium").otherwise("Low").alias("monthly_fee_band"),
    )
)
