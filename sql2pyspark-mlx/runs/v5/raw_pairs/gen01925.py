from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .select("email")
    .unionByName(df_employees.filter(F.col("manager_id").isNull()).select("email"))
    .distinct()
)
