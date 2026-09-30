from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "Singapore", "Bangkok"]) & F.col("dept_name").isin(["HR", "Finance"]))
    .groupBy(
        F.when(F.col("budget") >= 3777000, "high").when(F.col("budget") >= 1796000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
    .orderBy("budget_bucket")
)
