from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2023-04-18"))) & F.col("status").isin(["ACTIVE", "CANCELLED", "EXPIRED"]))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        F.floor("monthly_fee").alias("monthly_fee_whole"),
    )
)

result = (
    base
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.country").alias("country"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.monthly_fee")).alias("max_monthly_fee"),
    )
    .orderBy(F.col("max_monthly_fee").desc())
)
