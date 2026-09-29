from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-04-18").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
        F.when(F.col("monthly_fee") >= 212, "High").when(F.col("monthly_fee") >= 63, "Medium").otherwise("Low").alias("monthly_fee_tier"),
    )
)

result = (
    cleaned
    .select("id", "status", "monthly_fee", "monthly_fee_filled")
    .unpivot(
        ids=["id", "status"],
        values=["monthly_fee", "monthly_fee_filled"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
)
