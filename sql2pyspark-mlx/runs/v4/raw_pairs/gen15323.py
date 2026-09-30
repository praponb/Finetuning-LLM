from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name")
    .distinct()
    .orderBy("dept_name")
)
