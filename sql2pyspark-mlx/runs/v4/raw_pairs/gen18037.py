from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("level", "job_title")
w_recent = Window.partitionBy("job_title", "manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2024-02-07").cast("date")) & F.upper("email").like("%ANY%"))
    .withColumn("level_prefix", F.substring("level", 1, 2))
    .withColumn("hire_date_label", F.date_format("hire_date", "yyyy-MM-dd"))
    .withColumn("level_filled", F.coalesce("level", F.lit("DIRECT")))
)

with_windows = (
    valid
    .select(
        "*",
        F.max("salary").over(w_grp).alias("level_max_salary"),
        F.sum("salary").over(w_recent).alias("running_salary"),
    )
)

base = (
    with_windows
    .filter(F.length("email") < 7)
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "level_prefix",
        "hire_date_label",
        "level_filled",
        "level_max_salary",
        "running_salary",
        F.concat_ws(",", "email", "level").alias("email_level_key"),
        F.length("level").alias("level_length"),
    )
)

kept_keys = (
    base
    .select("job_title", "level")
    .subtract(
        df_employees
        .filter(~F.col("level").isin(["L4", "L1"]))
        .select("job_title", "level")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.budget"))
)

joined_employees = (
    joined
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.salary").alias("employee_salary"))
)

by_type = (
    joined_employees
    .groupBy("job_title")
    .pivot("level", ["L3", "L5"])
    .agg(F.avg("running_salary"))
    .select("job_title", F.col("L3").alias("l3"), F.col("L5").alias("l5"))
)

result = (
    by_type
    .select("job_title", "l3", "l5")
    .orderBy(F.col("l5").desc_nulls_last())
)
