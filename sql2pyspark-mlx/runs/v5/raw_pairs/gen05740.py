from pyspark.sql import functions as F

min_hire_date = df_employees.filter(F.col("dept_id") == 11).agg(F.min("hire_date")).first()[0]

result = (
    df_employees
    .filter(F.col("hire_date") <= min_hire_date)
    .select("id", "hire_date", "email")
    .orderBy("email", "id")
    .limit(50)
)
