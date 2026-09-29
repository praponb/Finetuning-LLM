from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(~F.col("location").isin(["London", "Berlin", "Chiang Mai"]))
    .withColumn("dept_name_upper", F.upper("dept_name"))
    .withColumn("location_group", F.when(F.col("location").isin(["Berlin", "Singapore"]), "Group A").otherwise("Other"))
)

kept = (
    base
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isNotNull()), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([10, 24, 38])), "left_anti")
)

scored = (
    eligible
    .select(
        "*",
        F.min("budget").over(w).alias("running_min_budget"),
        F.count("budget").over(w).alias("running_count_budget"),
        F.avg("budget").over(w).alias("running_avg_budget"),
    )
)

result = (
    scored
    .groupBy("location", "dept_name")
    .agg(
        F.percentile_approx("running_count_budget", [0.25, 0.5, 0.75]).alias("running_count_budget_quartiles"),
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_rows"),
        F.percentile_approx("running_count_budget", 0.95).alias("running_count_budget_p95"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("running_avg_budget"))).alias("marketing_total_running_avg_budget"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
)
