from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("dept_name")
w3 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .select("*", F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_filled"))
)

metrics = (
    base
    .withColumn("budget_avg_3_prior", F.avg("budget").over(w))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("first_known_location", F.first("location", ignorenulls=True).over(w3))
)

base_departments = (
    metrics
    .filter(F.col("budget") <= 2405000)
    .withColumn("budget_minus_budget_share", (F.col("budget") - F.col("budget_share")))
    .withColumn("least_budget_share_budget", F.least("budget_share", "budget"))
)

with_latest = (
    base_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"))
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("least_budget_share_budget") >= 90, "HIGH").otherwise("LOW").alias("least_budget_share_budget_tier"),
    )
    .agg(
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
    )
)
