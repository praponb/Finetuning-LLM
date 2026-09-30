from pyspark.sql import functions as F

result = (
    df_customers
    .select("email", F.lit("customer").alias("source"))
    .unionByName(df_employees.select("email", F.lit("employee").alias("source")))
)
