from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(
        F.col("d.dept_name"),
        F.when(F.col("d.budget") >= 4096000, "high").when(F.col("d.budget") >= 2046000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("e.level")))).alias("level_values"),
    )
    .orderBy("level_values", "budget_bucket")
)
