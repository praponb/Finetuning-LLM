from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4108000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.count("*").alias("num_rows"),
        F.stddev_samp("budget").alias("budget_stddev_2"),
        F.size(F.collect_set("location")).alias("num_locations"),
    )
    .orderBy(F.col("num_rows").desc())
)
