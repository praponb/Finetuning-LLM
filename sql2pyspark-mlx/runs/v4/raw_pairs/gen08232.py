from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") > "2025-03-01")
    .select("email", "hire_date", F.round("salary", 2).alias("salary_rounded"))
)
