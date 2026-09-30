from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("e.is_remote"))
    .groupBy(
        F.when(F.col("d.budget") >= 3671000, "high").when(F.col("d.budget") >= 1139000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("e.level") == "L2", F.col("e.salary")).otherwise(0)).alias("l2_salary"),
        F.sum(F.when(F.col("e.level") == "L4", F.col("e.salary")).otherwise(0)).alias("l4_salary"),
    )
    .filter(F.col("l2_salary") >= 49)
    .orderBy("l2_salary")
)
