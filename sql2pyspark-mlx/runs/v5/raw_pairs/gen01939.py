from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Corporate"]) & F.col("credit_limit").between(6200, 13000))
)

customers_stats = (
    valid_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"))
)

result = (
    customers_stats
    .filter(F.col("sg_count") < 9)
    .select("signup_month", "sg_count")
)
