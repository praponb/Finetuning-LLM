from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("location") != "Bangkok"))
    .groupBy(
        F.when(F.col("budget") >= 3376000, "high").when(F.col("budget") >= 1705000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"))
)
