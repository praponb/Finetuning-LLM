from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("name", "is_active", "email")
    .orderBy(F.col("name").desc(), "email")
)
