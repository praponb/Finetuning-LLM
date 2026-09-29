from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("dept_name") == "HR"))
    .select("*", F.coalesce("dept_name", F.lit("Unknown")).alias("dept_name_filled"))
)

result = (
    cleaned
    .groupBy("location", "dept_name")
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.max(F.when(F.col("dept_name") == "HR", F.col("budget"))).alias("hr_max_budget"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
    )
    .filter(F.col("num_locations") > 1)
)
