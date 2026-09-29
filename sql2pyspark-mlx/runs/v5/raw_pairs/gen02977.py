from pyspark.sql import functions as F

src = df_customers.filter(~F.col("segment").isin(["SMB", "Consumer"]))

agg = (
    src
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.count("*").alias("customer_count"),
        F.max("signup_date").alias("latest_signup_date"),
    )
)

result = (
    agg
    .filter(F.col("customer_count") < 4)
    .select("signup_month", "customer_count", "latest_signup_date")
    .orderBy("customer_count", "signup_month")
    .limit(20)
)
