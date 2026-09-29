from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc())
w2 = Window.partitionBy("manager_id").orderBy("salary", "id")
w3 = Window.partitionBy("dept_id").orderBy("salary_cume_dist", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("dept_id", "manager_id").orderBy(F.col("last7_max_salary").asc_nulls_last(), "id")

valid = (
    df_employees
    .filter((~F.col("level").eqNullSafe(F.lit("L5"))) & (F.col("hire_date") > F.lit("2023-04-28").cast("date")))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "is_remote",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("job_title", F.lit("OTHER")).alias("resolved_job_title"),
    )
)

scored = (
    valid
    .filter(~F.col("is_remote"))
    .withColumn("salary_cume_dist", F.cume_dist().over(w))
    .withColumn("next_salary", F.lead("salary", 2).over(w2))
)

with_dims = (
    scored
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.budget"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.avg("salary").over(w3).alias("last7_avg_salary"),
        F.max("salary").over(w3).alias("last7_max_salary"),
        F.sum("salary").over(w3).alias("last7_sum_salary"),
    )
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.full_name",
        "a.dept_id",
        "a.manager_id",
        "a.salary",
        "a.salary_cume_dist",
        "a.next_salary",
        "a.budget",
        "a.last7_avg_salary",
        "a.last7_max_salary",
        "a.rn",
        F.col("d.budget").alias("department_budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
)

result = (
    enriched
    .select(
        "id",
        "dept_id",
        "manager_id",
        "salary",
        "salary_cume_dist",
        "next_salary",
        "budget",
        "last7_avg_salary",
        "rn",
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("rn").desc())
)
