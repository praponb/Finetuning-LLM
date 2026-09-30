from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1234000, 2466000))
    .select("dept_name", "location")
    .distinct()
)
