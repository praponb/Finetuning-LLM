from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") < "2023-12-01")
    .orderBy("country")
)
