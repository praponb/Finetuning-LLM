from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 7900)
    .select("segment")
    .unionByName(df_customers.filter(F.upper("email").like("%LIC%")).select("segment"))
    .distinct()
)
