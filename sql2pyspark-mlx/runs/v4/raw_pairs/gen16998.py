from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name", "location")
    .distinct()
    .orderBy("dept_name", "location")
    .limit(1)
)
