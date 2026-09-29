from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Berlin")
    .orderBy("name", "signup_date")
)
