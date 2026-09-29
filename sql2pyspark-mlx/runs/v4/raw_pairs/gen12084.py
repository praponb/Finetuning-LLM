from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level", "job_title")
w3 = Window.partitionBy("level").orderBy("hire_date", "id")
w_part = Window.orderBy("hire_date", "id")
w_ordered = Window.partitionBy("dept_id").orderBy("level_avg_salary", "id")
w6 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.max("salary").alias("max_employee_salary"))

filtered = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select("*", F.substring("full_name", 1, 4).alias("full_name_prefix"))
)

scored = (
    filtered
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w))
    .withColumn("level_avg_salary", F.avg("salary").over(w2))
    .withColumn("prev_salary", F.lag("salary", 1).over(w3))
)

with_windows = (
    scored
    .withColumn("prev_salary_2", F.lag("salary", 1).over(w_part))
    .withColumn("next_salary", F.lead("salary", 1).over(w_ordered))
    .withColumn("level_max_level_avg_salary", F.max("level_avg_salary").over(w2))
    .withColumn("salary_bucket", F.ntile(3).over(w6))
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.budget"), F.col("b.dept_name"))
)

with_dims_employees = (
    with_dims
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .select(
        "s.*",
        F.col("e.level").alias("employee_level"),
        F.col("e.salary").alias("employee_salary"),
    )
)

kept_keys = (
    with_dims_employees
    .select("level")
    .intersect(
        df_employees
        .filter(F.col("manager_id").isNull())
        .select("level")
    )
)

kept_rows = (
    with_dims_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "full_name_prefix",
        "last_known_level",
        F.when(F.col("job_title").rlike("^(Manager|Analyst)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("job_title_zone"),
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("max_employee_salary").desc_nulls_first())
)
