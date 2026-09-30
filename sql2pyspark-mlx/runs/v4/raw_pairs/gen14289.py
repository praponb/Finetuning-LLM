from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "EXPIRED") & F.month("end_date").isin([7, 8, 11]))
    .select(
        "start_date",
        "end_date",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
    .orderBy(F.col("end_date").desc())
)
