from pyspark.sql import functions as F

remaining_keys = (
    df_departments
    .select("location", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isNotNull())
        .select("location", "dept_name")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_rows"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
    )
    .orderBy("sales_rows")
)
