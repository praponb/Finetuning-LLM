from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name", "location").orderBy(F.col("running_sum_budget").desc(), "id")
w3 = Window.partitionBy("dept_name", "location").orderBy("running_sum_budget", "id")

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("NONE")))
)

metrics = (
    cleaned
    .filter(F.col("dept_name").isin(["Marketing", "HR", "Finance"]))
    .select(
        "*",
        F.avg("budget").over(w_hist).alias("running_avg_budget"),
        F.max("budget").over(w_hist).alias("running_max_budget"),
        F.min("budget").over(w_hist).alias("running_min_budget"),
        F.sum("budget").over(w_hist).alias("running_sum_budget"),
    )
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.month(F.col("x.hire_date")).isin([9, 10, 11])), "left_anti")
)

metrics_departments = (
    eligible
    .withColumn("next_running_min_budget", F.lead("running_min_budget").over(w3))
    .withColumn("prev_running_min_budget", F.lag("running_min_budget").over(w3))
    .withColumn("prev_running_sum_budget", F.lag("running_sum_budget").over(w3))
    .withColumn("row_seq", F.row_number().over(w3))
)

result = (
    metrics_departments
    .select(
        "id",
        "dept_name",
        "location",
        "dept_name_filled",
        "running_avg_budget",
        "running_min_budget",
        "running_sum_budget",
        "dedup_rank",
        "next_running_min_budget",
    )
    .orderBy(F.col("next_running_min_budget").asc_nulls_first())
)
