from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last())
w_hist = Window.orderBy("budget", "id")
w5 = Window.partitionBy("dept_name").orderBy("budget", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("prev_budget_tier").orderBy(F.col("total_budget_dense_rank_whole").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.substring("location", 1, 2).alias("location_prefix"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.year(F.col("c.hire_date")) == 2024), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "dept_name", "budget", "location_prefix")
)

with_windows = (
    ranked
    .select(
        "id",
        "budget",
        F.avg("budget").over(w2).alias("budget_avg_2"),
        F.dense_rank().over(w_ordered).alias("budget_dense_rank"),
        F.lead("budget", 1).over(w_hist).alias("next_budget"),
        F.lag("budget", 2, 0).over(w5).alias("prev_budget"),
    )
)

filtered = (
    with_windows
    .filter(F.col("budget_avg_2") == 3006811)
    .select("*", F.ceil("budget_dense_rank").alias("budget_dense_rank_whole"))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

cleaned = (
    with_latest
    .filter(F.col("budget_dense_rank") == 4)
    .select(
        "id",
        "budget_dense_rank",
        "prev_budget",
        "budget_dense_rank_whole",
        "employee_hire_date",
        (F.col("prev_budget") * F.col("budget")).alias("prev_budget_times_budget"),
    )
)

totals = (
    cleaned
    .groupBy(
        F.when(F.col("prev_budget") >= 4130000, "HIGH").otherwise("LOW").alias("prev_budget_tier"),
        F.when(F.col("prev_budget_times_budget") >= 77, "HIGH").otherwise("LOW").alias("prev_budget_times_budget_tier"),
    )
    .agg(
        F.sum("budget_dense_rank_whole").alias("total_budget_dense_rank_whole"),
        F.percentile_approx("budget_dense_rank_whole", 0.9).alias("budget_dense_rank_whole_p90"),
        F.percentile_approx("prev_budget_times_budget", [0.25, 0.5, 0.75]).alias("prev_budget_times_budget_quartiles"),
    )
    .filter(F.col("budget_dense_rank_whole_p90") > 0)
    .withColumn("total_budget_dense_rank_whole_rank", F.rank().over(w7))
)

result = (
    totals
    .select(
        "prev_budget_tier",
        "prev_budget_times_budget_tier",
        "budget_dense_rank_whole_p90",
        "prev_budget_times_budget_quartiles",
        "total_budget_dense_rank_whole_rank",
    )
)
