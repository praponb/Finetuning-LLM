from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~(F.col("is_remote") & (F.col("salary") <= 92000)))
    .select("id", "salary", "email", "dept_id")
    .orderBy(F.col("dept_id").desc(), "id")
    .limit(1)
)
