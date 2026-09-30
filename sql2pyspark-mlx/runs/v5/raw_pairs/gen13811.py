from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select("id", "is_remote", F.date_add("hire_date", 80).alias("hire_date_plus_80d"))
)
