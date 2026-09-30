from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 61000)
    .select("opened_date")
    .union(df_employees.filter(F.col("is_remote")).select("hire_date"))
    .distinct()
)
