from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter((F.col("age") >= 47) & F.lower("name").like("%liv%") & F.col("is_active"))
)

signup_month_summary = (
    recent_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"), "country")
    .agg(
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
    )
)

result = (
    signup_month_summary
    .filter(F.col("new_york_count") > 53)
    .orderBy("new_york_count")
)
