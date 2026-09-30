from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id")

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.is_remote")), "left_anti")
)

cleaned = (
    kept
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.initcap(F.trim("location")).alias("location_title"))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") <= "2024-09-15"), "left_anti")
)

with_windows = (
    eligible
    .withColumn("last7_sum_budget", F.sum("budget").over(w))
    .withColumn("last7_avg_budget", F.avg("budget").over(w))
    .withColumn("last7_min_budget", F.min("budget").over(w))
    .withColumn("next_budget", F.lead("budget", 1).over(w2))
)

eligible_departments = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id").isNull()), "left_anti")
)

not_flagged = (
    eligible_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id") == 4), "left_anti")
)

totals = (
    not_flagged
    .withColumn("last7_min_budget_tier", F.when(F.col("last7_min_budget") >= 4040000, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["dept_name", "last7_min_budget_tier"], ["dept_name"], []],
        "dept_name",
        "last7_min_budget_tier",
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"),
        F.sum(F.when(F.col("location") == "Bangkok", F.col("last7_avg_budget")).otherwise(0)).alias("bangkok_last7_avg_budget"),
        F.round(F.avg("next_budget"), 2).alias("avg_next_budget"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_rows"),
        F.round(F.avg("next_budget"), 2).alias("avg_next_budget_2"),
    )
)

result = (
    totals
    .select(
        "dept_name",
        "last7_min_budget_tier",
        "sales_count",
        "avg_next_budget",
        "hr_rows",
        "avg_next_budget_2",
    )
    .orderBy(F.col("sales_count").desc(), F.col("last7_min_budget_tier").desc())
)
