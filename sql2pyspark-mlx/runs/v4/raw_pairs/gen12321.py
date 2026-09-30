from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([14, 36]) & (F.col("dept_id") == 13))
    .select("id", "email")
    .orderBy("id")
)
