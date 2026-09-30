from pyspark.sql import functions as F

src = df_departments.filter(F.col("dept_name") != "Legal")

departments_stats = (
    src
    .groupBy("dept_name")
    .agg(F.sum("budget").alias("total_budget"))
)

result = departments_stats.orderBy("dept_name")
