from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_employees
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.budget"))
)

scored = (
    joined
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.first("job_title", ignorenulls=True).over(w2).alias("first_known_job_title"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        "e.*",
        F.col("d.location"),
        F.col("d.budget").alias("department_budget"),
        F.col("d.dept_name"),
    )
)

remaining_keys = (
    enriched
    .select("id")
    .intersect(
        df_employees
        .filter(F.col("hire_date") >= "2025-10-01")
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

by_type = (
    kept_rows
    .groupBy("dept_id")
    .pivot("location", ["Singapore", "Bangkok"])
    .agg(F.max("department_budget").alias("value_sum"))
)

result = by_type.select("dept_id")
