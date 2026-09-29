from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.avg("age").alias("avg_age"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 4)
    .select("country", "signup_month", "avg_age", "row_count")
    .orderBy("row_count")
)
