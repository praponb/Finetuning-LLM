from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 10800)
    .select("name")
    .union(df_employees.select("full_name"))
)
