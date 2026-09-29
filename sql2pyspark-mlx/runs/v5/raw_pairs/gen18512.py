from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .unionByName(df_employees.select(F.col("hire_date").alias("launch_date")))
    .distinct()
    .orderBy("launch_date")
)
