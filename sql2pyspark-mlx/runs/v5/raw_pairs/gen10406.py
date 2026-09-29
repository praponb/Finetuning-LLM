from pyspark.sql import functions as F

result = (
    df_departments
    .rollup(
        F.when(F.col("budget") >= 3816000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.count_if(F.col("location") == "Chiang Mai").alias("chiang_mai_events"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget"))).alias("singapore_total_budget"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
    .orderBy(F.col("singapore_total_budget").desc())
)
