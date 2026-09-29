from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 180) & F.month("start_date").isin([6, 8, 10]))
    .select(
        "id",
        "monthly_fee",
        "start_date",
        "end_date",
        F.when(F.col("plan").isin(["enterprise", "basic"]), "Tier 1").otherwise("Other").alias("plan_group"),
    )
    .orderBy(F.col("id").desc(), F.col("monthly_fee").desc())
    .limit(5)
)
