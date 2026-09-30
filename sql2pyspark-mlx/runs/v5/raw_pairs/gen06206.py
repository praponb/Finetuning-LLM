from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "HR", "Marketing"]))
    .groupBy("dept_name")
    .agg(F.sum("budget").alias("total_budget"))
    .filter(F.col("total_budget") < 7155000)
    .orderBy("total_budget", "dept_name")
    .limit(3)
)
