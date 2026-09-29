from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy("location").orderBy("budget")
w_running = Window.partitionBy("location").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("location").orderBy(F.col("hire_date").asc_nulls_first(), "id")
w7 = Window.partitionBy("location").orderBy("cumulative_budget", "id")
w8 = Window.orderBy("row_seq", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("location").orderBy("budget_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.location", "s.budget", F.col("c.hire_date"))
)

scored = (
    with_latest
    .select(
        "*",
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.row_number().over(w3).alias("row_seq"),
        F.rank().over(w4).alias("budget_rank"),
        F.sum("budget").over(w_running).alias("cumulative_budget"),
    )
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored_departments = (
    latest
    .select(
        "id",
        "budget",
        "hire_date",
        "row_seq",
        "budget_rank",
        F.lag("cumulative_budget", 1).over(w7).alias("prev_cumulative_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share_2"),
        F.count("budget_share").over(w2).alias("location_count_budget_share"),
        F.avg("cumulative_budget").over(w8).alias("cumulative_budget_avg_3_prior"),
        F.sum("budget").over(w9).alias("running_budget"),
    )
)

result = (
    scored_departments
    .select(
        "id",
        "budget",
        "hire_date",
        "row_seq",
        "prev_cumulative_budget",
        "budget_share_2",
        "location_count_budget_share",
        "cumulative_budget_avg_3_prior",
        "running_budget",
    )
    .orderBy(F.col("cumulative_budget_avg_3_prior").desc())
)
