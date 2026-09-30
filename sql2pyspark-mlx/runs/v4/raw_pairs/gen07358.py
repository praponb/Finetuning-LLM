from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2025-08-01")
    .select("opened_date")
    .intersect(df_employees.select("hire_date"))
    .orderBy(F.col("opened_date").desc())
)
