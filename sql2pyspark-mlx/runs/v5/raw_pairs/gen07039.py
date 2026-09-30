from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.lit("account").alias("source"), "opened_date")
    .union(df_employees.select(F.lit("employee").alias("source"), "hire_date"))
    .distinct()
)
