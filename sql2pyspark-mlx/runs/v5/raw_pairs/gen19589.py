from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([8, 19, 29, 39]) & (F.col("salary") < 94000))
    .select("id", "manager_id")
)
