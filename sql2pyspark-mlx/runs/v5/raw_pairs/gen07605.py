from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("level").orderBy("salary", "id")
w4 = Window.partitionBy("dept_id", "level").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("manager_id").orderBy("running_avg_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("salary_share").desc())
w7 = Window.partitionBy("location", "dept_id").orderBy("hire_date", "id")
w8 = Window.partitionBy("location", "dept_id").orderBy("running_min_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter((~F.col("level").eqNullSafe(F.lit("L1"))) & F.col("manager_id").isin([1, 32, 37]))
    .select(
        "*",
        F.coalesce("full_name", F.lit("unassigned")).alias("full_name_filled"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name").isNotNull())
    .select("s.*", F.col("d.location"))
)

metrics = (
    enriched
    .select(
        "*",
        F.max("salary").over(w).alias("running_max_salary"),
        F.min("salary").over(w).alias("running_min_salary"),
        F.avg("salary").over(w).alias("running_avg_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

enriched_employees = (
    ranked
    .withColumn("running_running_avg_salary", F.sum("running_avg_salary").over(w4))
    .withColumn("last_known_running_max_salary", F.last("running_max_salary", ignorenulls=True).over(w5))
    .withColumn("salary_share_rank", F.rank().over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
    .withColumn("last_known_running_min_salary", F.last("running_min_salary", ignorenulls=True).over(w8))
)

enriched_employees_2 = (
    enriched_employees
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.budget"), F.col("b.dept_name"))
)

result = (
    enriched_employees_2
    .groupBy(
        "job_title",
        F.when(F.col("running_min_salary") >= 140000, "LARGE").otherwise("SMALL").alias("running_min_salary_tier"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("running_avg_salary"))).alias("chiang_mai_total_running_avg_salary"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.count_if(F.col("job_title") == "Designer").alias("designer_events"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.max(F.when(F.col("level") == "L5", F.col("seq_no"))).alias("l5_max_seq_no"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
    )
    .orderBy(F.col("running_min_salary_tier").desc())
)
