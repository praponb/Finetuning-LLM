from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("engineering_budget").desc())

matched_keys = (
    df_departments
    .select("id", "dept_name")
    .intersect(
        df_departments
        .filter(F.col("dept_name").isin(["Finance", "Marketing"]))
        .select("id", "dept_name")
    )
)

kept_rows = (
    df_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4483000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
        F.sum("budget").alias("total_budget"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count_2"),
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("budget")).otherwise(0)).alias("engineering_budget"),
    )
    .withColumn("engineering_budget_percent_rank", F.percent_rank().over(w))
)
