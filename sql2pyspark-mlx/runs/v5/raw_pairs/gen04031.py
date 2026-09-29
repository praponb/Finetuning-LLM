from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .withColumn("monthly_fee_adjusted", F.round((F.col("monthly_fee") * 100), 2))
    .withColumn("monthly_fee_adjusted_2", F.round((F.col("monthly_fee") * 1.1), 2))
)

result = (
    filtered
    .select(
        "id",
        "plan",
        "monthly_fee_adjusted",
        "monthly_fee",
        "monthly_fee_adjusted_2",
    )
    .unpivot(
        ids=["id", "plan"],
        values=["monthly_fee_adjusted", "monthly_fee", "monthly_fee_adjusted_2"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
