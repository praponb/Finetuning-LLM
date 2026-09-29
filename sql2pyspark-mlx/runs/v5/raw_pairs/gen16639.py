from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("level").orderBy("prev_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("level", "job_title").orderBy("next_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("job_title", "level").orderBy(F.col("budget").asc_nulls_last())
w_hist = Window.partitionBy("level", "job_title").orderBy("prev_budget", "id")
w7 = Window.orderBy(F.col("next_budget").desc(), "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("employee_level").orderBy("running_budget", "id")
w_running = Window.partitionBy("employee_level").orderBy("cumulative_prev_budget", "id")
w11 = Window.orderBy("cumulative_prev_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy(F.col("running_budget").desc(), "id")

valid = (
    df_departments
    .filter(~F.col("location").eqNullSafe(F.lit("Bangkok")))
    .withColumn("budget_band", F.when(F.col("budget") >= 3895000, "High").when(F.col("budget") >= 1456000, "Medium").otherwise("Low"))
)

scored = (
    valid
    .filter(F.col("budget").isNotNull())
    .withColumn("prev_budget", F.lag("budget", 1).over(w))
    .withColumn("next_budget", F.lead("budget", 2).over(w))
)

with_last = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        "s.prev_budget",
        "s.next_budget",
        F.col("c.job_title"),
        F.col("c.level"),
    )
)

enriched = (
    with_last
    .select(
        "id",
        "prev_budget",
        "next_budget",
        "job_title",
        "level",
        F.sum("prev_budget").over(w3).alias("cumulative_prev_budget"),
        F.sum("budget").over(w_part).alias("running_budget"),
        F.percent_rank().over(w5).alias("budget_percent_rank"),
        F.lag("next_budget", 1).over(w_hist).alias("prev_next_budget"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

with_last_departments = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.prev_budget",
        "s.next_budget",
        "s.cumulative_prev_budget",
        "s.running_budget",
        "s.budget_percent_rank",
        F.col("c.level").alias("employee_level"),
        F.col("c.hire_date"),
    )
)

enriched_departments = (
    with_last_departments
    .select(
        "id",
        "cumulative_prev_budget",
        "running_budget",
        F.lag("prev_budget").over(w_ordered).alias("prev_prev_budget"),
        F.lead("prev_budget").over(w_ordered).alias("next_prev_budget"),
        F.lag("cumulative_prev_budget").over(w_ordered).alias("prev_cumulative_prev_budget"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

with_windows = (
    enriched_departments
    .withColumn("cumulative_prev_cumulative_prev_budget", F.sum("prev_cumulative_prev_budget").over(w11))
    .withColumn("event_seq_2", F.row_number().over(w12))
)

totals = (
    with_windows
    .groupBy(
        F.when(F.col("next_prev_budget") >= 3131000, "HIGH").otherwise("LOW").alias("next_prev_budget_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") > 1)
)

result = (
    totals
    .select("next_prev_budget_tier", "num_rows")
    .orderBy(F.col("next_prev_budget_tier").desc(), "num_rows")
)
