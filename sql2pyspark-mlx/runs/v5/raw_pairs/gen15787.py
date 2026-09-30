from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy(F.col("salary").desc(), "id")
w_running = Window.partitionBy("location").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("running_count_salary").desc(), "id")
w4 = Window.partitionBy("location").orderBy("salary", "id")
w5 = Window.orderBy(F.col("salary").desc(), "id")
w6 = Window.partitionBy("location").orderBy("running_avg_salary", "id").rowsBetween(-3, -1)

base = (
    df_employees
    .filter((~F.col("level").eqNullSafe(F.lit("L5"))) & F.col("email").like("%@gmail.com"))
    .withColumn("salary_scaled", F.round((F.col("salary") * 0.9), 2))
)

joined = (
    base
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.full_name",
        "a.email",
        "a.dept_id",
        "a.manager_id",
        "a.job_title",
        "a.salary",
        "a.hire_date",
        "a.level",
        F.col("p.dept_name"),
        F.col("p.location"),
        F.col("p.budget"),
    )
)

latest = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "dept_name",
        "location",
        F.avg("salary").over(w_running).alias("running_avg_salary"),
        F.count("salary").over(w_running).alias("running_count_salary"),
        F.sum("salary").over(w_running).alias("running_sum_salary"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "left")
    .filter(F.col("d.hire_date") < "2024-03-23")
    .select(
        "a.*",
        F.col("d.full_name"),
        F.col("d.hire_date").alias("employee_hire_date"),
        F.col("d.level"),
    )
)

latest_employees = (
    with_dims
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "salary",
        "location",
        "running_avg_salary",
        "running_count_salary",
        "running_sum_salary",
        "employee_hire_date",
    )
)

metrics = (
    latest_employees
    .select(
        "id",
        "location",
        "running_count_salary",
        "running_sum_salary",
        F.lead("salary", 1, 0).over(w4).alias("next_salary"),
        F.row_number().over(w5).alias("seq_no"),
        F.avg("salary").over(w6).alias("salary_avg_3_prior"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("running_count_salary") >= 15, "LARGE").otherwise("SMALL").alias("running_count_salary_tier"),
        F.when(F.col("next_salary") >= 150000, "HIGH").otherwise("LOW").alias("next_salary_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count_if(F.col("location") == "Tokyo").alias("tokyo_events"),
    )
    .filter(F.col("cnt") < 1000)
)
