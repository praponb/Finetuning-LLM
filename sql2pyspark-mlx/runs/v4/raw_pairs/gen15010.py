from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "dept_name", F.upper("dept_name").alias("dept_name_upper"))
)
