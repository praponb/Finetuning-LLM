from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") >= 62)
    .select("name")
    .union(df_employees.select("full_name"))
    .distinct()
)
