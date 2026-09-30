from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3180000, "high").when(F.col("budget") >= 1648000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("budget_range") >= 73)
)
