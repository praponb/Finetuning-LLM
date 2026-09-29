from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("location").isNotNull())
    .withColumn("budget_scaled", F.round((F.col("budget") * 1.07), 2))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("NONE")))
)

result = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary") >= 87000), "left_anti")
)
