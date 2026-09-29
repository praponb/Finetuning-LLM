from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("level").orderBy(F.col("salary_rounded").asc_nulls_last())
w3 = Window.partitionBy("dept_id")
w4 = Window.partitionBy("manager_id").orderBy(F.col("salary_rounded").asc_nulls_last())

matched_keys = (
    df_employees
    .select("job_title")
    .subtract(
        df_employees
        .filter(F.col("hire_date") < "2025-05-01")
        .select("job_title")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(~F.col("job_title").eqNullSafe(F.lit("Intern")))
    .withColumn("salary_rounded", F.round("salary", 2))
    .withColumn("level_filled", F.coalesce("level", F.lit("UNKNOWN")))
)

valid = (
    filtered
    .filter(F.col("hire_date") > F.lit("2023-04-02").cast("date"))
    .select(
        "*",
        F.round((F.col("salary_rounded") * 1.1), 2).alias("salary_rounded_scaled"),
    )
)

with_windows = (
    valid
    .filter(F.col("dept_id") == 26)
    .select(
        "*",
        F.rank().over(w).alias("salary_rank"),
        F.percent_rank().over(w2).alias("salary_rounded_percent_rank"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
        F.dense_rank().over(w4).alias("salary_rounded_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "level",
        "salary",
        "salary_share",
        "salary_rounded_scaled",
        "salary_rounded_percent_rank",
    )
    .unpivot(
        ids=["level"],
        values=["salary", "salary_share", "salary_rounded_scaled", "salary_rounded_percent_rank"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
