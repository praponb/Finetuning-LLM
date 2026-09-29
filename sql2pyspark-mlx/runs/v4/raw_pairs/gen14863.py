from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 4500)
    .select("id", "country", "age", "is_active")
)
