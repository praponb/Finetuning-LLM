from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        "dept_name",
        F.when(F.col("budget") >= 3853000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles_2"),
        F.avg("budget").alias("avg_budget"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.sum("budget").alias("total_budget"),
        F.max("budget").alias("max_budget"),
    )
    .orderBy(F.col("total_budget").desc_nulls_first())
)
