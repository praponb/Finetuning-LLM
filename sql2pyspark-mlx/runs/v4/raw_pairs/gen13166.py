from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((~F.col("city").isin(["Bangkok", "Chiang Mai", "Tokyo"]) & F.col("name").like("No%")) | F.col("signup_date").between("2023-07-09", "2024-04-01"))
)

summary = (
    filtered_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
    )
)

result = (
    summary
    .select("signup_month", "segment_values")
    .orderBy(F.col("signup_month").desc())
)
