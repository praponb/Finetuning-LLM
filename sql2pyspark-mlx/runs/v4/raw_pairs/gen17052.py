from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "ACTIVE"]) | (F.col("end_date") < "2025-06-01"))
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "status",
        F.round((F.col("monthly_fee") * 0.85), 2).alias("monthly_fee_scaled"),
    )
    .orderBy("start_date")
)
