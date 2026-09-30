from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .orderBy("hire_date")
)
