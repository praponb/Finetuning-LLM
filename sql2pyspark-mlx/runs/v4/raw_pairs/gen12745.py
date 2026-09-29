from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select("email")
    .unionByName(df_employees.filter(F.col("dept_id").isin([1, 2, 18])).select("email"))
)
