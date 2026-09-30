from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") >= "2024-11-01")
    .select("email", F.lit("customer").alias("source"))
    .unionByName(
        df_employees
        .filter(F.col("level") == "L1")
        .select("email", F.lit("employee").alias("source"))
    )
    .distinct()
    .orderBy("email")
)
