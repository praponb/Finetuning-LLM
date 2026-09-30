from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .intersect(df_employees.select(F.col("hire_date").alias("opened_date")))
    .orderBy("opened_date")
)
