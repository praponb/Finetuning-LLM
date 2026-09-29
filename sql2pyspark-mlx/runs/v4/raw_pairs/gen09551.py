from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .groupBy("dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0))) / F.count("*")), 1).alias("pct_hr"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .filter(F.col("budget_range") >= 20)
    .orderBy(F.col("pct_hr").desc(), F.col("dept_name").desc())
)
