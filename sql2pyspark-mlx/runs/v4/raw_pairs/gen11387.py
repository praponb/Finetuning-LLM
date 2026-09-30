from pyspark.sql import functions as F

result = (
    df_customers
    .select("email")
    .unionByName(df_employees.filter(F.col("manager_id").isin([2, 3])).select("email"))
    .orderBy(F.col("email").desc())
)
