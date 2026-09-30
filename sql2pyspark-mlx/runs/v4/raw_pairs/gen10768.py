from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3210000, "high").when(F.col("budget") >= 1971000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.countDistinct("budget").alias("unique_dept_names"),
        F.count("budget").alias("budget_count"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("max_budget") < 3076000)
    .drop("max_budget")
)
