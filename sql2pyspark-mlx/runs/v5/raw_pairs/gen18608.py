from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .union(df_accounts.select("opened_date"))
    .distinct()
)
