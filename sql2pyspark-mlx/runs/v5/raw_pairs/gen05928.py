from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.month(F.col("x.hire_date")).isin([2, 5, 12])), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "dept_name",
        "location",
        F.when(F.col("dept_name").rlike("^(HR|Engineering)$"), "PRIORITY").otherwise("GROUP_A").alias("dept_name_zone"),
    )
    .orderBy(F.col("location").desc(), "dept_name")
)
