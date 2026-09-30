from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Tokyo", "New York"]) | (F.col("age") > 59))
    .select("signup_date", "segment")
)
