from pyspark.sql import functions as F

src = df_employees.filter(F.col("email").isNotNull() & F.col("email").isNotNull())

agg = (
    src
    .groupBy("dept_id")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)

result = (
    agg
    .select("dept_id", "latest_hire_date")
    .orderBy("latest_hire_date", F.col("dept_id").desc())
)
