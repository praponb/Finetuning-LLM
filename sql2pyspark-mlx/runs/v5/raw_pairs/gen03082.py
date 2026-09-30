from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") < 380)
    .select("launch_date")
    .union(df_employees.select("hire_date"))
    .orderBy("launch_date")
)
