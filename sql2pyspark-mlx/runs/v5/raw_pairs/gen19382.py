from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("manager_id").orderBy("salary_share", "id")
w4 = Window.orderBy("salary_share", "id")
w5 = Window.orderBy(F.col("next_salary").desc(), "id")
w6 = Window.orderBy("salary", "id")

with_windows = (
    df_employees
    .filter(F.col("level").isin(["L5", "L2", "L4"]))
    .select(
        "*",
        F.lead("salary").over(w_ordered).alias("next_salary"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .select(
        "a.id",
        "a.manager_id",
        "a.job_title",
        "a.salary",
        "a.hire_date",
        "a.level",
        "a.next_salary",
        "a.salary_share",
        F.col("e.job_title").alias("employee_job_title"),
    )
)

enriched = (
    joined
    .withColumn("salary_share_bucket", F.ntile(4).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

joined_employees = (
    enriched
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.manager_id",
        "a.job_title",
        "a.salary",
        "a.level",
        "a.next_salary",
        "a.salary_share",
        "a.seq_no",
        F.col("d.full_name"),
        F.col("d.level").alias("employee_level"),
    )
)

enriched_employees = (
    joined_employees
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "e.*",
        F.col("d.full_name").alias("employee_full_name"),
        F.col("d.email"),
        F.col("d.job_title").alias("employee_job_title"),
    )
)

metrics = (
    enriched_employees
    .select(
        "id",
        "manager_id",
        "job_title",
        "level",
        "next_salary",
        "salary_share",
        "full_name",
        "employee_level",
        "employee_job_title",
        F.ntile(3).over(w5).alias("next_salary_bucket"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("salary_share") >= 0.61, "LARGE").otherwise("SMALL").alias("salary_share_tier"),
    )
    .agg(
        F.round(F.avg("next_salary_bucket"), 2).alias("avg_next_salary_bucket"),
        F.percentile_approx("next_salary", 0.5).alias("next_salary_p50"),
        F.count_if(F.col("employee_job_title") == "Analyst").alias("analyst_events"),
        F.count_if(F.col("employee_job_title") == "Analyst").alias("analyst_events_2"),
        F.sum(F.when(F.col("employee_job_title") == "Director", F.col("next_salary_bucket"))).alias("director_total_next_salary_bucket"),
    )
    .orderBy(F.col("salary_share_tier").asc_nulls_last())
)
