from pyspark.sql import functions as F

not_flagged = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary").between(71000, 127000)), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("budget") >= 4219000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_rows"),
        F.stddev_samp("budget").alias("budget_stddev_2"),
    )
    .orderBy(F.col("berlin_rows").desc())
)
