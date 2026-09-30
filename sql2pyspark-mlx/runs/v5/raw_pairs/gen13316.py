from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(31, 36))
    .select("credit_limit", "segment", "age")
)
