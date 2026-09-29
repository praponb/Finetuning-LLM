from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "outer")
    .groupBy(
        F.when(F.col("departments.dept_name").isin(["Sales", "Marketing"]), "group_a").otherwise("other").alias("dept_name_group"),
        F.col("departments.location"),
    )
    .agg(F.countDistinct(F.col("employees.salary")).alias("unique_managers"))
    .orderBy("location", "dept_name_group")
    .limit(1)
)
