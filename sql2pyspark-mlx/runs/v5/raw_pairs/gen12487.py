from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("stddev_budget") < 72)
    .select("location", "stddev_budget", "dept_name_list")
    .orderBy(F.col("stddev_budget").desc(), "location")
)
