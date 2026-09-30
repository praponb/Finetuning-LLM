from pyspark.sql import functions as F

result = (
    df_customers
    .select("email", F.lit("customer").alias("source"))
    .unionByName(
        df_employees
        .filter(F.year("hire_date") == 2024)
        .select("email", F.lit("employee").alias("source"))
    )
    .orderBy("source")
)
