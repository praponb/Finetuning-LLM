from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("name").isNull() & (F.length("name") >= 5))

summary = (
    recent_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "TH", 1).otherwise(0))) / F.count("*")), 1).alias("pct_th"),
        F.count("*").alias("total_count"),
    )
)

result = (
    summary
    .select("signup_year", "pct_th", "total_count")
    .orderBy(F.col("signup_year").desc(), "pct_th")
)
