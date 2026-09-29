from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Finance"]) & F.col("dept_name").isin(["Sales", "Engineering"]))
    .groupBy(
        F.when(F.col("budget") >= 3495000, "high").when(F.col("budget") >= 1237000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
    .orderBy("budget_bucket", "budget_range")
)
