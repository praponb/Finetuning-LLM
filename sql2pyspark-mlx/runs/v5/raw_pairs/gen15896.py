from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.round((F.col("budget") * 0.85), 2).alias("adjusted_budget"),
        F.when(F.col("dept_name").isin(["Marketing", "Legal"]), "Priority").otherwise("Other").alias("dept_name_group"),
    )
)

result = (
    filtered
    .withColumn("adjusted_budget_tier", F.when(F.col("adjusted_budget") >= 79, "HIGH").otherwise("LOW"))
    .rollup("location", "adjusted_budget_tier")
    .agg(
        F.min("budget").alias("min_budget"),
        F.percentile_approx("adjusted_budget", [0.25, 0.5, 0.75]).alias("adjusted_budget_quartiles"),
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_count"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.grouping_id().alias("gid"),
    )
)
