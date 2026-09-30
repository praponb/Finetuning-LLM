from pyspark.sql import functions as F

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.lower(F.col("c.email")).like("%ob.%")), "left_semi")
)

result = (
    kept
    .groupBy("dept_name")
    .agg(
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy(F.col("budget_p95").desc_nulls_last())
)
