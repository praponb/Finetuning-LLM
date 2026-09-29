from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "CANCELLED"]))
    .select(
        "id",
        "end_date",
        "monthly_fee",
        F.date_add("end_date", 68).alias("end_date_plus_68d"),
    )
)
