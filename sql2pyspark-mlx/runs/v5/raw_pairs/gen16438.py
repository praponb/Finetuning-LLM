from pyspark.sql import functions as F

base = df_customers.filter(F.col("is_active") | (F.col("country") == "DE"))

summary = (
    base
    .groupBy(F.month("signup_date").alias("signup_month"), "country")
    .agg(F.count("*").alias("num_customers"))
)

result = (
    summary
    .select("signup_month", "country", "num_customers")
    .orderBy("country")
)
