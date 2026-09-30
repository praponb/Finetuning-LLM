from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy("manager_id")
w3 = Window.partitionBy("job_title")
w_recent = Window.partitionBy("level").orderBy(F.col("manager_id_count_salary_per_salary_share").desc_nulls_first())
w5 = Window.partitionBy("manager_id", "level").orderBy(F.col("manager_id_count_salary_per_salary_share").desc_nulls_last())

kept_keys = (
    df_employees
    .select("level")
    .intersectAll(
        df_employees
        .filter(F.col("level").isin(["L2", "L4"]))
        .select("level")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .filter(F.col("email").like("%@startup.io"))
    .withColumn("dept_id_min_salary", F.min("salary").over(w))
    .withColumn("manager_id_count_salary", F.count("salary").over(w2))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w3)), 4))
)

with_windows = (
    scored
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "is_remote",
        "level",
        "dept_id_min_salary",
        "manager_id_count_salary",
        "salary_share",
        F.avg("salary").over(w).alias("dept_id_avg_salary"),
    )
)

valid = (
    with_windows
    .filter(F.col("level").isNotNull() & F.col("manager_id_count_salary").between(8, 16))
    .withColumn("manager_id_count_salary_per_salary_share", (F.col("manager_id_count_salary") / F.nullif("salary_share", F.lit(0))))
    .withColumn("resolved_level", F.coalesce("level", F.lit("OTHER")))
)

enriched = (
    valid
    .select(
        "*",
        F.dense_rank().over(w_recent).alias("manager_id_count_salary_per_salary_share_dense_rank"),
        F.percent_rank().over(w5).alias("manager_id_count_salary_per_salary_share_percent_rank"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .select(
        "a.*",
        F.col("e.level").alias("employee_level"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.salary"),
    )
)

base = (
    joined
    .filter(F.col("salary_share") <= 0.35)
    .select(
        "*",
        F.coalesce("employee_full_name", F.lit("N/A")).alias("employee_full_name_filled"),
        F.coalesce("full_name", F.lit("unassigned")).alias("full_name_filled"),
    )
)

result = (
    base
    .select(
        "manager_id",
        "manager_id_count_salary_per_salary_share_percent_rank",
        "salary",
    )
    .unpivot(
        ids=["manager_id"],
        values=["manager_id_count_salary_per_salary_share_percent_rank", "salary"],
        variableColumnName="metric",
        valueColumnName="val",
    )
)
