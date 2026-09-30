from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("budget").between(1241000, 2646000))
    .select(
        "*",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)

result = (
    valid
    .withColumn("budget_tier", F.when(F.col("budget") >= 4054000, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["budget_tier", "dept_name"], ["budget_tier"], []],
        "budget_tier",
        "dept_name",
    )
    .agg(
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("budget"))).alias("chiang_mai_total_budget"),
        F.count_if(F.col("dept_name") == "HR").alias("hr_events"),
    )
)
