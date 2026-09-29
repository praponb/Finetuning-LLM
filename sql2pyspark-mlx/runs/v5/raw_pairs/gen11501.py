from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 15200)
    .select("email")
    .subtract(df_employees.filter(F.col("is_remote")).select("email"))
)
