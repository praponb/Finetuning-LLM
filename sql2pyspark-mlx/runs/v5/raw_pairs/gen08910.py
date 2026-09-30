from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") >= 109000)
    .select("id", "hire_date", "dept_id")
    .unionByName(
        df_employees
        .filter(F.col("level") == "L1")
        .select("id", "hire_date", "dept_id")
    )
    .distinct()
)
