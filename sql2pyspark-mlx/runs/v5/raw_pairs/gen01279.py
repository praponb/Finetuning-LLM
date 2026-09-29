from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") >= 105000) & (F.year("hire_date") == 2025))
    .select("is_remote")
)
