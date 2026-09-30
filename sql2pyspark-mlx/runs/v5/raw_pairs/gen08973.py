from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("running_min_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level", "dept_id").orderBy("running_min_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("manager_id").orderBy("running_min_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "level",
        F.avg("salary").over(w).alias("running_avg_salary"),
        F.max("salary").over(w).alias("running_max_salary"),
        F.min("salary").over(w).alias("running_min_salary"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.last("level", ignorenulls=True).over(w2).alias("last_known_level"),
        F.sum("salary").over(w3).alias("running_salary"),
    )
)

cleaned = (
    metrics
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "running_min_salary",
        F.round((F.col("salary") * 0.85), 2).alias("salary_adjusted"),
        F.coalesce("level", F.lit("N/A")).alias("level_filled"),
    )
)

scored = (
    cleaned
    .filter(F.col("full_name").isNull())
    .select(
        "*",
        F.min("running_min_salary").over(w_grp).alias("running_min_running_min_salary"),
        F.avg("running_min_salary").over(w_grp).alias("running_avg_running_min_salary"),
        F.max("running_min_salary").over(w_grp).alias("running_max_running_min_salary"),
        F.sum("running_min_salary").over(w_grp).alias("running_sum_running_min_salary"),
    )
)

result = (
    scored
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.dept_name").alias("dept_name"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.running_avg_running_min_salary")).alias("avg_running_avg_running_min_salary"),
    )
    .orderBy(F.col("avg_running_avg_running_min_salary").desc())
)
