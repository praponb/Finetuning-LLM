from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .select("email")
    .unionByName(df_employees.select("email"))
    .distinct()
)
