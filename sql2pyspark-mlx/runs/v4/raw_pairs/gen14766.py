from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (~F.col("x.email").like("%@yahoo.com")), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("budget") >= 3120000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
        "dept_name",
    )
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_rows"),
    )
    .orderBy(F.col("budget_tier").desc_nulls_first())
)
