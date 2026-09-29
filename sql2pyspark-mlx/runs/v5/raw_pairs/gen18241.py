from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("employee_job_title").orderBy("budget", "id")
w_recent = Window.partitionBy("location").orderBy("budget", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("level").orderBy("budget_bucket", "id")
w6 = Window.orderBy("prev_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("level").orderBy("prev_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("dept_name")
w9 = Window.partitionBy("dept_name").orderBy("prev_budget", "id")

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.job_title").alias("employee_job_title"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary") < 128000), "left_semi")
)

scored = (
    not_flagged
    .filter(F.col("level").isin(["L1", "L5"]))
    .select(
        "id",
        "dept_name",
        "level",
        F.lead("budget").over(w_part).alias("next_budget"),
        F.lag("budget").over(w_part).alias("prev_budget"),
        F.row_number().over(w_part).alias("row_seq"),
        F.ntile(10).over(w_recent).alias("budget_bucket"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.level",
        "s.prev_budget",
        "s.budget_bucket",
        F.col("c.job_title").alias("employee_job_title"),
    )
)

metrics = (
    latest_match
    .select(
        "*",
        F.lead("prev_budget", 1, 0).over(w5).alias("next_prev_budget"),
        F.sum("budget_bucket").over(w6).alias("running_budget_bucket"),
        F.last("dept_name", ignorenulls=True).over(w7).alias("last_known_dept_name"),
        F.round((F.col("prev_budget") / F.sum("prev_budget").over(w8)), 4).alias("prev_budget_share"),
        F.lead("prev_budget", 2, 0).over(w9).alias("next_prev_budget_2"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("prev_budget_share") >= 0.88, "HIGH").otherwise("LOW").alias("prev_budget_share_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("level") == "L3").alias("l3_events"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
    .orderBy(F.col("l3_events").desc_nulls_first())
)
