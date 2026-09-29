from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy("salary", "id")
w_grp = Window.partitionBy("level").orderBy("salary", "id")
w3 = Window.partitionBy("manager_id")
w4 = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

metrics = (
    df_employees
    .withColumn("salary_bucket", F.ntile(4).over(w))
    .withColumn("next_salary", F.lead("salary", 1).over(w_grp))
    .withColumn("manager_id_min_salary", F.min("salary").over(w3))
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    ranked
    .select("dept_id", "job_title")
    .subtract(
        df_employees
        .filter(F.col("level").isin(["L5", "L4"]))
        .select("dept_id", "job_title")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.job_title").alias("employee_job_title"),
        F.col("b.level").alias("employee_level"),
    )
)

enriched = (
    with_dims
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.location"), F.col("p.dept_name"), F.col("p.budget"))
)

result = (
    enriched
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        "next_salary",
        "manager_id_min_salary",
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("job_title").desc_nulls_last(), "manager_id")
)
