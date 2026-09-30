from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(
        F.col("d.location"),
        F.when(F.col("d.budget") >= 3201000, "high").when(F.col("d.budget") >= 1306000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct(F.col("e.salary")).alias("unique_levels"))
    .orderBy("unique_levels")
)
