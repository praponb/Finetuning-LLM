from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "location")

joined = (
    df_employees
    .alias("s")
    .join(df_employees.alias("p"), F.col("s.manager_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.full_name").alias("employee_full_name"))
)

joined_employees = (
    joined
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.full_name",
        "s.email",
        "s.dept_id",
        "s.is_remote",
        "s.level",
        "s.employee_full_name",
        F.col("d.dept_name"),
        F.col("d.location"),
    )
)

with_dims = (
    joined_employees
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "inner")
    .select(
        "src.*",
        F.col("p.budget"),
        F.col("p.location").alias("department_location"),
    )
)

matched_keys = (
    with_dims
    .select("dept_id", "id")
    .subtract(
        df_employees
        .filter(F.col("manager_id").isNull())
        .select("dept_id", "id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget").between(2041000, 3206000))
    .select(
        "a.id",
        "a.full_name",
        "a.dept_id",
        "a.is_remote",
        "a.level",
        "a.dept_name",
        "a.location",
        "a.budget",
        F.col("d.dept_name").alias("department_dept_name"),
        F.col("d.location").alias("department_location_2"),
        F.col("d.budget").alias("department_budget"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "full_name",
        "level",
        "department_dept_name",
        F.max("budget").over(w).alias("level_max_budget"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("level").isNotNull())
    .select(
        "*",
        F.when(F.col("department_dept_name").isin(["Engineering", "HR"]), "Core").otherwise("Other").alias("department_dept_name_group"),
    )
)

result = cleaned.select("id", "full_name", "department_dept_name", "level_max_budget")
