from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isNotNull()), "left_semi")
)

base = (
    kept
    .filter(F.col("dept_name").isNotNull() & (F.col("budget") <= 1896000))
    .select(
        "*",
        F.coalesce("dept_name", F.lit("Unknown")).alias("dept_name_filled"),
        F.ceil("budget").alias("budget_whole"),
        F.coalesce("dept_name", F.lit("N/A")).alias("resolved_dept_name"),
    )
)

kept_departments = (
    base
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.upper(F.col("ch.email")).like("%MMA%")), "left_anti")
)

deduped = (
    kept_departments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .groupBy("location")
    .agg(
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
        F.sum(F.when(F.col("dept_name") == "HR", F.col("budget")).otherwise(0)).alias("hr_budget"),
        F.round(F.avg("budget_whole"), 2).alias("avg_budget_whole"),
    )
    .filter(F.col("avg_budget_whole") >= 0)
    .orderBy(F.col("hr_budget").asc_nulls_last())
)
