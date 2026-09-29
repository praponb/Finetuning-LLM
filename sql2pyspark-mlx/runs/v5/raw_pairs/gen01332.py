from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"))
)

result = (
    with_latest
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("budget")).otherwise(0)).alias("engineering_budget"),
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_count"),
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .orderBy(F.col("chiang_mai_count").asc_nulls_first())
)
