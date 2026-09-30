from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("dept_name") == "Marketing"))
    .select("id", "dept_name", "budget")
)
