from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("age", "segment", "signup_date")
    .orderBy("signup_date")
)
