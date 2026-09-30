from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("first_known_budget").desc(), "id")
w4 = Window.orderBy("budget", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("employee_level").orderBy(F.col("budget").desc(), "id")

enriched = (
    df_departments
    .select(
        "id",
        "budget",
        F.avg("budget").over(w).alias("budget_avg_3"),
        F.first("budget", ignorenulls=True).over(w2).alias("first_known_budget"),
    )
)

base = (
    enriched
    .filter(F.col("budget_avg_3").between(2551548, 3022884))
    .withColumn("first_known_budget_times_budget", (F.col("first_known_budget") * F.col("budget")))
)

with_windows = (
    base
    .select(
        "id",
        "budget",
        "first_known_budget_times_budget",
        F.ntile(10).over(w3).alias("first_known_budget_bucket"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        "s.first_known_budget_times_budget",
        F.col("c.level").alias("employee_level"),
    )
)

eligible = (
    latest_match
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.job_title").isNull()), "left_semi")
)

latest = (
    eligible
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

totals = (
    latest
    .groupBy(
        F.when(F.col("first_known_budget_times_budget") >= 81, "LARGE").otherwise("SMALL").alias("first_known_budget_times_budget_tier"),
    )
    .agg(
        F.avg(F.when(F.col("employee_level") == "L5", F.col("budget"))).alias("l5_avg_budget"),
        F.avg(F.when(F.col("employee_level") == "L5", F.col("first_known_budget_times_budget"))).alias("l5_avg_first_known_budget_times_budget"),
        F.sort_array(F.collect_set("employee_level")).alias("employee_level_list"),
        F.count(F.when(F.col("employee_level") == "L1", 1)).alias("l1_rows"),
        F.sum(F.when(F.col("employee_level") == "L3", F.col("first_known_budget_times_budget")).otherwise(0)).alias("l3_first_known_budget_times_budget"),
        F.count_if(F.col("employee_level") == "L1").alias("l1_events"),
    )
)

result = (
    totals
    .select(
        "first_known_budget_times_budget_tier",
        "employee_level_list",
        "l1_rows",
        "l3_first_known_budget_times_budget",
        "l1_events",
    )
    .orderBy(F.col("l3_first_known_budget_times_budget").asc_nulls_first())
)
