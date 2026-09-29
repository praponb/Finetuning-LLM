from pyspark.sql import functions as F

max_hire_date = df_employees.filter(F.col("email").like("%@mail.co.th")).agg(F.max("hire_date")).first()[0]

result = (
    df_employees
    .filter(F.col("hire_date") == max_hire_date)
    .select("id", "hire_date", "dept_id", "salary")
)
