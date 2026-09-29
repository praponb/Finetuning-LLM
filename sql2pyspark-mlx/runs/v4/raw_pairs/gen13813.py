from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@gmail.com"))
    .select("id", "manager_id", "dept_id", "salary")
    .orderBy("salary")
)
