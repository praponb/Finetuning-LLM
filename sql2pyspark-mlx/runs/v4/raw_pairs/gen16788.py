from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("name").like("%smith%"))
    .select("id", "email", "name", "credit_limit")
)
