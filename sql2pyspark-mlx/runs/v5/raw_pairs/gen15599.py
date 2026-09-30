from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 5)
    .select("id", "level", "dept_id", F.floor("salary").alias("salary_whole"))
    .orderBy("dept_id", "id")
    .limit(10)
)
