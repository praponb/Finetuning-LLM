from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title")
w4 = Window.partitionBy("dept_id").orderBy("hire_date", "id")

cleaned = (
    df_employees
    .filter(F.col("salary") < 130000)
    .withColumn("level_prefix", F.substring("level", 1, 4))
    .withColumn("clean_full_name", F.trim("full_name"))
)

with_windows = (
    cleaned
    .filter(F.col("salary") >= 98000)
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("running_salary", F.sum("salary").over(w2))
    .withColumn("salary_share_2", F.round((F.col("salary") / F.sum("salary").over(w3)), 4))
    .withColumn("salary_share_3", F.round((F.col("salary") / F.sum("salary").over(w3)), 4))
    .withColumn("prev_salary", F.lag("salary", 1, 0).over(w4))
)

remaining_keys = (
    with_windows
    .select("dept_id", "manager_id")
    .subtract(
        df_employees
        .filter(F.col("level").isin(["L2", "L4"]))
        .select("dept_id", "manager_id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("dept_id")
    .intersectAll(
        df_employees
        .filter(~F.col("is_remote"))
        .select("dept_id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.job_title").alias("employee_job_title"))
)

result = (
    joined
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "inner")
    .groupingSets(
        [["employee_job_title", "employee_level"], ["employee_job_title"]],
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.salary")).alias("max_salary"),
        F.round(F.avg(F.col("a.salary_share_3")), 2).alias("avg_salary_share_3"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("max_salary")
)
