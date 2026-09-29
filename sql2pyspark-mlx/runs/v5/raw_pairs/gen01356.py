from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("dept_id", "manager_id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-09-09").cast("date")) & (F.year("hire_date") == 2023))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.trim("job_title").alias("clean_job_title"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "dept_id", "manager_id", "salary", "is_remote", "level")
)

with_windows = (
    ranked
    .select(
        "*",
        F.first("level", ignorenulls=True).over(w2).alias("first_known_level"),
        F.ntile(5).over(w3).alias("salary_bucket"),
        F.max("salary").over(w4).alias("dept_id_max_salary"),
    )
)

kept_keys = (
    with_windows
    .select("dept_id", "manager_id")
    .exceptAll(
        df_employees
        .filter(F.col("dept_id") == 34)
        .select("dept_id", "manager_id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("id", "dept_id")
    .intersect(
        df_employees
        .filter(F.col("is_remote"))
        .select("id", "dept_id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(~F.col("level").eqNullSafe(F.lit("L2")))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "salary",
        "is_remote",
        "first_known_level",
        "salary_bucket",
        "dept_id_max_salary",
        F.when(F.col("level").isin(["L4", "L5"]), "Core").otherwise("Other").alias("level_group"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_filled"),
    )
)

result = (
    base
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "manager_id",
        "salary",
        "first_known_level",
        "dept_id_max_salary",
        "level_group",
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("min_employee_salary").desc_nulls_last(), "id")
)
