from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("country") == "TH")

customers_stats = (
    recent_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"))
)

result = (
    customers_stats
    .filter(F.col("stddev_credit_limit") > 26)
    .select("signup_year", "stddev_credit_limit")
)
