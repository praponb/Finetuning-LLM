from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "level").orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id", "job_title").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("dept_id", "manager_id")
w4 = Window.partitionBy("dept_name", "manager_id").orderBy(F.col("prev_salary").desc_nulls_first())

metrics = (
    df_employees
    .select(
        "*",
        F.lag("salary", 2).over(w).alias("prev_salary"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("salary").over(w3).alias("dept_id_avg_salary"),
    )
)

enriched = (
    metrics
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.email",
        "e.dept_id",
        "e.manager_id",
        "e.prev_salary",
        "e.seq_no",
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("d.location"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.email").alias("employee_email"))
)

deduped = (
    joined
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined_employees = (
    deduped
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .select("a.*", F.col("e.level"), F.col("e.job_title"), F.col("e.full_name"))
)

result = (
    joined_employees
    .withColumn("prev_salary_tier", F.when(F.col("prev_salary") >= 127000, "PREMIUM").otherwise("STANDARD"))
    .withColumn("budget_tier", F.when(F.col("budget") >= 3446000, "HIGH").otherwise("LOW"))
    .rollup("dept_name", "prev_salary_tier", "budget_tier")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget")).otherwise(0)).alias("bangkok_budget"),
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_count"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("budget_p50") > 0)
    .orderBy(F.col("prev_salary_tier").asc_nulls_first())
)
