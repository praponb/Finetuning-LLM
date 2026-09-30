from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Legal"]))
    .groupBy(
        F.when(F.col("dept_name").isin(["Engineering", "Finance"]), "priority").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.count("*").alias("row_count"),
    )
)
