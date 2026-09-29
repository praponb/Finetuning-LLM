from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("department_location").orderBy(F.col("budget").desc_nulls_first(), "id")

enriched = (
    df_employees
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.full_name",
        "src.dept_id",
        "src.manager_id",
        "src.job_title",
        "src.hire_date",
        "src.is_remote",
        "src.level",
        F.col("b.location"),
        F.col("b.budget"),
    )
)

enriched_employees = (
    enriched
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.full_name",
        "src.dept_id",
        "src.manager_id",
        "src.is_remote",
        "src.level",
        "src.location",
        F.col("d.dept_name"),
    )
)

enriched_employees_2 = (
    enriched_employees
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.full_name",
        "e.dept_id",
        "e.manager_id",
        "e.level",
        "e.dept_name",
        F.col("d.location").alias("department_location"),
        F.col("d.dept_name").alias("department_dept_name"),
    )
)

with_dims = (
    enriched_employees_2
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.full_name",
        "a.dept_id",
        "a.level",
        "a.dept_name",
        "a.department_location",
        "a.department_dept_name",
        F.col("d.dept_name").alias("department_dept_name_2"),
        F.col("d.budget"),
    )
)

ranked = (
    with_dims
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

joined = (
    ranked
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        "e.*",
        F.col("d.dept_name").alias("department_dept_name_3"),
        F.col("d.budget").alias("department_budget"),
        F.col("d.location"),
    )
)

result = (
    joined
    .select("id", "department_dept_name_2", "budget", "department_budget")
    .unpivot(
        ids=["id", "department_dept_name_2"],
        values=["budget", "department_budget"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
