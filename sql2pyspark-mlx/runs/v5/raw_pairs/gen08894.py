from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("budget", "id")
w3 = Window.partitionBy("employee_level").orderBy(F.col("budget").desc_nulls_last())
w4 = Window.partitionBy("dept_id")
w5 = Window.partitionBy("location").orderBy(F.col("budget_cume_dist").desc())
w6 = Window.partitionBy("location")
w7 = Window.partitionBy("employee_level", "dept_name")
w_grp = Window.partitionBy("employee_level").orderBy(F.col("budget_cume_dist").asc_nulls_last())
w9 = Window.orderBy("salary")
w10 = Window.orderBy(F.col("budget").asc_nulls_last())
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.avg("salary").alias("avg_employee_salary"))

with_dims = (
    df_employees
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "inner")
    .select(
        "s.*",
        F.col("e.level").alias("employee_level"),
        F.col("e.full_name").alias("employee_full_name"),
    )
)

with_dims_employees = (
    with_dims
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name").isin(["Legal", "Marketing", "Sales"]))
    .select(
        "src.id",
        "src.full_name",
        "src.email",
        "src.dept_id",
        "src.salary",
        "src.hire_date",
        "src.is_remote",
        "src.employee_level",
        "src.employee_full_name",
        F.col("d.budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
)

enriched = (
    with_dims_employees
    .select(
        "*",
        F.first("location", ignorenulls=True).over(w).alias("first_known_location"),
        F.lag("budget", 2).over(w2).alias("prev_budget"),
        F.cume_dist().over(w3).alias("budget_cume_dist"),
    )
)

matched_keys = (
    enriched
    .select("id", "dept_id")
    .intersectAll(
        df_employees
        .filter(F.col("is_remote"))
        .select("id", "dept_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("prev_budget_share", F.round((F.col("prev_budget") / F.sum("prev_budget").over(w4)), 4))
    .withColumn("budget_cume_dist_rank", F.rank().over(w5))
    .withColumn("location_count_prev_budget", F.count("prev_budget").over(w6))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w7)), 4))
)

metrics_employees = (
    metrics
    .select(
        "*",
        F.rank().over(w_grp).alias("budget_cume_dist_rank_2"),
        F.rank().over(w9).alias("salary_rank"),
        F.percent_rank().over(w10).alias("budget_percent_rank"),
    )
)

with_dims_employees_2 = (
    metrics_employees
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.full_name",
        "a.email",
        "a.dept_id",
        "a.salary",
        "a.hire_date",
        "a.is_remote",
        "a.employee_level",
        "a.employee_full_name",
        "a.budget",
        "a.dept_name",
        "a.prev_budget_share",
        "a.budget_cume_dist_rank",
        "a.location_count_prev_budget",
        "a.salary_share",
        "a.salary_rank",
        "a.budget_percent_rank",
        F.col("p.dept_name").alias("department_dept_name"),
        F.col("p.budget").alias("department_budget"),
    )
)

pivoted = (
    with_dims_employees_2
    .groupBy("dept_id", "dept_name")
    .pivot("department_dept_name", ["Marketing", "Sales"])
    .agg(F.avg("salary").alias("amount"))
)

result = (
    pivoted
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "dept_id",
        "dept_name",
        "Sales",
        F.when(F.col("dept_name").rlike("^(HR|Finance)$"), "GROUP_B").when(F.col("dept_name").rlike("^(Engineering|Sales)$"), "CORE").otherwise("PRIORITY").alias("dept_name_zone"),
        F.col("avg_employee_salary"),
    )
    .orderBy(F.col("avg_employee_salary").desc_nulls_last(), F.col("dept_name").desc())
)
