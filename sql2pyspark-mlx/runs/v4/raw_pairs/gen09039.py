from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & (F.col("signup_date") >= "2023-08-01"))
    .select("id", "age", "country")
    .orderBy(F.col("age").desc())
)
