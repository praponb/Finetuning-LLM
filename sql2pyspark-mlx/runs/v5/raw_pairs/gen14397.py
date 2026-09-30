from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location").isin(["New York", "Singapore", "London"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .filter(F.upper(F.col("e.email")).like("%IAM%"))
    .select(F.col("e.id"), F.col("e.full_name"), F.col("e.salary"))
    .orderBy("full_name")
)
