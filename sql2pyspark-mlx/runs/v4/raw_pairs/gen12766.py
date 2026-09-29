from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") < 12)
    .select(F.col("hire_date").alias("event_date"))
    .unionByName(df_products.select(F.col("launch_date").alias("event_date")))
    .distinct()
)
