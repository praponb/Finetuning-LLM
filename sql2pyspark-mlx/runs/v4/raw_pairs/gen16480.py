from pyspark.sql import functions as F

base = df_departments.filter(F.col("budget") <= 1845000)

departments_stats = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3226000, "high").when(F.col("budget") >= 1986000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("department_count"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
)

result = (
    departments_stats
    .filter(F.col("department_count") < 7)
    .select("budget_bucket", "department_count")
    .orderBy("budget_bucket")
)
