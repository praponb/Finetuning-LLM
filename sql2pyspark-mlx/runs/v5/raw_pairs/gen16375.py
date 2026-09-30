from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(F.col("dept_name") == "Engineering").alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_semi")
    .filter(F.upper(F.trim(F.col("employees.email"))) == "CAROL@COMPANY.CO")
    .select(F.col("employees.id"), F.col("employees.salary"), F.col("employees.email"))
    .orderBy(F.col("salary").desc())
)
