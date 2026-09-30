from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("budget").between(2908000, 3079000) & (F.col("budget") < 2786000))
)

departments_stats = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3611000, "high").when(F.col("budget") >= 1979000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"))
)

result = departments_stats
