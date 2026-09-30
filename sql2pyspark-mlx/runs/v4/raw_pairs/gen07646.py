from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4206000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.count_if(F.col("dept_name") == "Marketing").alias("marketing_events"),
    )
    .orderBy(F.col("location").asc_nulls_first())
)
