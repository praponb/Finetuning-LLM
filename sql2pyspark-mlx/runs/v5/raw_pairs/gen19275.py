from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 160) & F.col("plan").isin(["basic", "enterprise", "standard"]))
    .select(
        "id",
        "status",
        F.when(F.col("monthly_fee") >= 204, "High").when(F.col("monthly_fee") >= 122, "Medium").otherwise("Low").alias("monthly_fee_level"),
    )
    .orderBy(F.col("id").desc(), "monthly_fee_level")
)
