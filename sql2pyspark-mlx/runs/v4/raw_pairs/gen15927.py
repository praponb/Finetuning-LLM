from pyspark.sql import functions as F

result = (
    df_customers
    .select("name")
    .subtract(
        df_employees
        .filter(F.year("hire_date") == 2024)
        .select(F.col("full_name").alias("name"))
    )
)
