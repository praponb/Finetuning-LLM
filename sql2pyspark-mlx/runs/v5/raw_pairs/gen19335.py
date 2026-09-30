from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@GMAIL.COM"))
    .select("email", F.lit("customer").alias("origin"))
    .unionByName(df_employees.select("email", F.lit("employee").alias("origin")))
    .orderBy("email")
)
