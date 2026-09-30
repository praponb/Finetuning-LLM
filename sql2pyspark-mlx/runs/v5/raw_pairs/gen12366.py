from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([1, 6, 12]) & (F.col("monthly_fee") < 180) & F.col("plan").isin(["basic", "enterprise"]))
    .select(
        "id",
        "start_date",
        F.date_sub("start_date", 77).alias("start_date_minus_77d"),
    )
)
