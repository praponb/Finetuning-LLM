from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.hire_date").alias("employee_hire_date"),
        F.col("c.job_title"),
    )
)

result = (
    latest_match
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3715000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "Tokyo", 1)), F.lit(0)), 2).alias("budget_per_tokyo"),
    )
    .orderBy(F.col("distinct_job_titles").asc_nulls_first())
)
