from pyspark.sql import functions as F

result = (
    df_customers
    .select("email")
    .intersect(df_employees.select("email"))
    .orderBy(F.col("email").desc())
)
