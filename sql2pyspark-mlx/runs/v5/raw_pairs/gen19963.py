from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .union(df_products.select("launch_date"))
)
