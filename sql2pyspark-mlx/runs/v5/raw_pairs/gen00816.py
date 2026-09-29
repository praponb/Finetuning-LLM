from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(F.col("city").isin(["Chiang Mai", "New York", "Tokyo"]) & F.col("country").isin(["TH", "SG"]))
)

is_active_summary = (
    recent_customers
    .groupBy("is_active")
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
)

result = (
    is_active_summary
    .select("is_active", "avg_credit_limit")
    .orderBy("avg_credit_limit", "is_active")
    .limit(3)
)
