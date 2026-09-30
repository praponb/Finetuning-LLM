from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.budget") >= 1567000)
    .groupBy(
        F.col("d.location"),
        F.when(F.col("d.dept_name").isin(["Sales", "Marketing"]), "core").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.sum(F.col("e.salary")).alias("total_salary"),
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
    )
    .orderBy("salary_range")
)
