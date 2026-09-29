from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("dept_name").orderBy("rn", "id")
w6 = Window.partitionBy("dept_name")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("location", "level").orderBy(F.col("dept_name_sum_budget").desc(), "id")
w9 = Window.partitionBy("location").orderBy("dept_name_sum_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("level").orderBy("event_seq", "id")
w11 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Sales", "Legal"]))
    .select(
        "*",
        F.coalesce("dept_name", F.lit("Unknown")).alias("dept_name_filled"),
        F.coalesce("dept_name", F.lit("NONE")).alias("resolved_dept_name"),
    )
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .filter(F.col("dept_name") == "HR")
    .select(
        "*",
        F.sum("budget").over(w2).alias("running_budget"),
        F.row_number().over(w3).alias("event_seq"),
        F.last("location", ignorenulls=True).over(w4).alias("last_known_location"),
        F.lag("budget", 1).over(w_running).alias("prev_budget"),
        F.sum("budget").over(w6).alias("dept_name_sum_budget"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w7))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.level"))
)

with_windows = (
    latest_match
    .select(
        "id",
        "location",
        "resolved_dept_name",
        "rn",
        "event_seq",
        "last_known_location",
        "level",
        F.ntile(4).over(w8).alias("dept_name_sum_budget_bucket"),
        F.last("dept_name_sum_budget", ignorenulls=True).over(w9).alias("last_known_dept_name_sum_budget"),
        F.row_number().over(w10).alias("row_seq"),
    )
)

latest_match_departments = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w11))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.location",
        "s.resolved_dept_name",
        "s.rn",
        "s.last_known_dept_name_sum_budget",
        F.col("c.hire_date"),
    )
)

result = (
    latest_match_departments
    .withColumn("last_known_dept_name_sum_budget_tier", F.when(F.col("last_known_dept_name_sum_budget") >= 15692950, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "hire_date"))
    .groupingSets(
        [["last_known_dept_name_sum_budget_tier", "quarter_start"], ["last_known_dept_name_sum_budget_tier"]],
        "last_known_dept_name_sum_budget_tier",
        "quarter_start",
    )
    .agg(
        F.max(F.when(F.col("location") == "Tokyo", F.col("last_known_dept_name_sum_budget"))).alias("tokyo_max_last_known_dept_name_sum_budget"),
        F.sum("last_known_dept_name_sum_budget").alias("total_last_known_dept_name_sum_budget"),
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_rows"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.min("last_known_dept_name_sum_budget").alias("min_last_known_dept_name_sum_budget"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("tokyo_max_last_known_dept_name_sum_budget") > 0)
    .orderBy("last_known_dept_name_sum_budget_tier")
)
