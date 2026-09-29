from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.concat("status", F.lit("/"), "plan").alias("status_plan"),
        F.round((F.col("monthly_fee") * 1.15), 2).alias("monthly_fee_adjusted"),
    )
)

result = (
    filtered
    .groupBy(F.date_trunc("quarter", "end_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("monthly_fee_per_enterprise"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
    )
    .filter(F.col("basic_rows") < 1)
)
