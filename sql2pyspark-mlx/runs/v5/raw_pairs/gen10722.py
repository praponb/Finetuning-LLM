from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(((F.col("plan") == "enterprise") & (F.col("monthly_fee") < 180)) | (F.col("end_date") >= "2024-07-04"))
    .select(
        "status",
        "monthly_fee",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)
