from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name", "location", "budget")
    .orderBy("dept_name", "location")
)
