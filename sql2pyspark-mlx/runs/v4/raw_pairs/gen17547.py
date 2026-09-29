from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") < 12)
    .select("id", "email", "manager_id", "dept_id")
)
