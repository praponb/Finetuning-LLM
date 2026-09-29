from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary")
w_ordered = Window.orderBy(F.col("employee_salary").desc_nulls_last())
w3 = Window.orderBy(F.col("department_budget_2").desc_nulls_last())
w_recent = Window.partitionBy("location").orderBy(F.col("department_budget_2").desc_nulls_first())

matched_keys = (
    df_employees
    .select("manager_id", "level")
    .intersect(
        df_employees
        .filter(F.col("salary") > 100000)
        .select("manager_id", "level")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .filter(F.col("salary").between(100000, 120000))
    .select("*", F.percent_rank().over(w).alias("salary_percent_rank"))
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select(
        "s.dept_id",
        "s.manager_id",
        "s.salary",
        "s.is_remote",
        "s.level",
        "s.salary_percent_rank",
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
    )
)

enriched = (
    with_dims
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.location").alias("department_location"),
        F.col("b.budget").alias("department_budget"),
    )
)

with_dims_employees = (
    enriched
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.salary").alias("employee_salary"))
)

joined = (
    with_dims_employees
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "inner")
    .select(
        "src.dept_id",
        "src.is_remote",
        "src.location",
        "src.dept_name",
        "src.budget",
        "src.department_location",
        "src.employee_salary",
        F.col("p.budget").alias("department_budget_2"),
        F.col("p.dept_name").alias("department_dept_name"),
    )
)

enriched_employees = (
    joined
    .withColumn("employee_salary_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("department_budget_2_rank", F.rank().over(w3))
    .withColumn("department_budget_2_percent_rank", F.percent_rank().over(w_recent))
)

result = (
    enriched_employees
    .select(
        "is_remote",
        "location",
        "dept_name",
        "budget",
        "department_location",
        "employee_salary",
        "department_budget_2",
        "employee_salary_percent_rank",
        "department_budget_2_rank",
    )
)
