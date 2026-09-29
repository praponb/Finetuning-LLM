from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"), F.col("c.job_title"))
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("budget") >= 4271000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("budget").alias("max_budget"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_rows"),
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
    )
    .filter(F.col("max_budget") < 1000000)
    .orderBy("budget_tier")
)
