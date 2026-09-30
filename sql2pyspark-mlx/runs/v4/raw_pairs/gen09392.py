from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .distinct()
    .crossJoin(df_employees.select("job_title").distinct())
)
