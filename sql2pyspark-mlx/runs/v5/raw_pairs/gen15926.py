from pyspark.sql import functions as F

with_dims = (
    df_employees
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.dept_id",
        "e.salary",
        "e.hire_date",
        "e.level",
        F.col("b.location"),
        F.col("b.budget"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "salary",
        "hire_date",
        "level",
        "location",
        F.when(F.col("level").rlike("^(L3|L5)$"), "GROUP_A").otherwise("GROUP_B").alias("level_zone"),
    )
    .orderBy("level_zone")
)
