from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .unionByName(df_employees.select(F.col("hire_date").alias("opened_date")))
    .distinct()
)
