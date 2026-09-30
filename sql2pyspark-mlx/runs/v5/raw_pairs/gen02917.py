from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("location") == "Tokyo") | F.col("budget").between(1272000, 2263000))
)

summary = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 3724000, "high").when(F.col("budget") >= 1109000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0))) / F.count("*")), 1).alias("pct_marketing"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
)

result = (
    summary
    .select("budget_bucket", "location", "pct_marketing", "location_list")
    .orderBy(F.col("budget_bucket").desc(), "location")
    .limit(10)
)
