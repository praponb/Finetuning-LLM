from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("manager_id").orderBy("hire_date_plus_months", "id")
w3 = Window.partitionBy("employee_job_title").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy(F.col("hire_date_plus_months").desc(), "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-08-03"))) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.add_months("hire_date", 6).alias("hire_date_plus_months"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.budget"))
)

joined = (
    with_dims
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id").eqNullSafe(F.col("e.id")), "inner")
    .select(
        "src.*",
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.salary"),
        F.col("e.hire_date").alias("employee_hire_date"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "is_remote_label",
        "budget",
        "salary",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.lead("budget", 1, 0).over(w2).alias("next_budget"),
        F.avg("budget").over(w3).alias("budget_avg_3"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

result = (
    metrics
    .cube("job_title", F.date_trunc("YEAR", "hire_date").alias("year_start"))
    .agg(
        F.avg(F.when(F.col("job_title") == "Director", F.col("salary"))).alias("director_avg_salary"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_rows"),
        F.count_if(F.col("job_title") == "Analyst").alias("analyst_events"),
    )
    .orderBy(F.col("distinct_manager_ids").asc_nulls_first())
)
