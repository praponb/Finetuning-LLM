from pyspark.sql import functions as F

result = (
    df_departments
    .select("location")
    .union(df_customers.filter(F.col("age").between(50, 67)).select("city"))
    .distinct()
)
