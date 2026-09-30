from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") > 108000) & (F.col("hire_date").between("2024-12-28", "2025-01-01") | ~F.col("is_remote")))
)
