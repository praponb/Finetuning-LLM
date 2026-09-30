from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.orderBy(F.col("budget").desc_nulls_first())
w3 = Window.orderBy("budget", "id")
w_grp = Window.partitionBy("location").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("prev_budget", "id")

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title") == "Intern"), "left_anti")
)

metrics = (
    eligible
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.lag("budget").over(w).alias("prev_budget"),
        F.lead("budget").over(w).alias("next_budget"),
        F.row_number().over(w).alias("seq_no"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date").between("2025-04-01", "2025-06-06")), "left_anti")
)

with_windows = (
    kept
    .select(
        "*",
        F.percent_rank().over(w2).alias("budget_percent_rank"),
        F.ntile(4).over(w3).alias("budget_bucket"),
        F.last("prev_budget", ignorenulls=True).over(w_grp).alias("last_known_prev_budget"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

filtered = (
    with_windows
    .filter(F.col("seq_no") >= 3)
    .select(
        "*",
        F.when(F.col("last_known_prev_budget") >= 3085000, "High").when(F.col("last_known_prev_budget") >= 1438000, "Medium").otherwise("Low").alias("last_known_prev_budget_tier"),
        F.coalesce("dept_name", F.lit("NONE")).alias("resolved_dept_name"),
    )
)

result = (
    filtered
    .groupBy("dept_name")
    .pivot("location", ["Bangkok", "Tokyo"])
    .agg(F.max("next_budget"))
)
