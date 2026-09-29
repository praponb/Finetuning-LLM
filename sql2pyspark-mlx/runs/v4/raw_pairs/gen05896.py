from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.substring("dept_name", 1, 3).alias("dept_name_prefix"),
        F.trim("dept_name").alias("clean_dept_name"),
        F.coalesce("dept_name", F.lit("N/A")).alias("resolved_dept_name"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.length(F.col("ch.full_name")) >= 8), "left_semi")
)

filtered = (
    eligible
    .filter(F.col("dept_name").isin(["Legal", "Finance", "Sales"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_prefix",
        "clean_dept_name",
        "resolved_dept_name",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
)

remaining_keys = (
    filtered
    .select("location")
    .exceptAll(
        df_departments
        .filter(F.col("budget").between(2814000, 3341000))
        .select("location")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .cube(
        F.when(F.col("budget") >= 3816000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.count_if(F.col("location") == "Singapore").alias("singapore_events"),
        F.avg("budget").alias("avg_budget"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.max(F.when(F.col("dept_name") == "Sales", F.col("budget"))).alias("sales_max_budget"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("singapore_events") >= 1)
    .orderBy(F.col("avg_budget").desc_nulls_first())
)
