from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") <= "2023-07-01")
    .groupBy("country")
    .pivot("city", ["Berlin", "New York"])
    .agg(F.count("credit_limit"))
    .orderBy("country")
)
