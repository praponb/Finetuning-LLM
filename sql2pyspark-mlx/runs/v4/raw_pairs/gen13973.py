from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNull() & F.col("full_name").like("%Car%"))
    .select("id", "full_name", "email", F.round("salary", 1).alias("salary_rounded"))
)
