from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("plan").isin(["enterprise", "basic"]))
    .select(
        "*",
        F.date_trunc("month", "start_date").alias("start_date_period"),
        F.round((F.col("monthly_fee") * 1.15), 2).alias("monthly_fee_scaled"),
    )
)

result = (
    cleaned
    .select("id", "customer_id", "monthly_fee_scaled", "monthly_fee")
    .unpivot(
        ids=["id", "customer_id"],
        values=["monthly_fee_scaled", "monthly_fee"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
