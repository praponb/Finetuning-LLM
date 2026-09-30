from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(33, 41) | ~F.col("email").like("%@yahoo.com"))
    .select("country", "email", "credit_limit")
)
