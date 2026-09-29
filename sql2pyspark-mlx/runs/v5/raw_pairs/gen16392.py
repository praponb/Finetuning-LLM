from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        F.col("monthly_fee").cast("int").alias("monthly_fee_int_2"),
    )
)
