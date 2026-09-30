from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") > 11800) & (F.col("country") != "US"))
    .select("country", "is_active", "age")
    .orderBy("country")
)
