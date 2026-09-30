from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isin([13, 18, 23])), "left_semi")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("budget") >= 3593000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        "location",
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget")).otherwise(0)).alias("sales_budget"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .filter(F.col("budget_stddev") > 0)
    .orderBy(F.col("location").desc())
)
