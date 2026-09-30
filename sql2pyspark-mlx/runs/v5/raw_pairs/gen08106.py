from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("manager_id", "job_title").orderBy("salary", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("salary").desc(), "id")

with_dims = (
    df_employees
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id").eqNullSafe(F.col("e.id")), "left")
    .select(
        "s.*",
        F.col("e.level").alias("employee_level"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.salary").alias("employee_salary"),
    )
)

enriched = (
    with_dims
    .filter(F.col("job_title").isin(["Analyst", "Intern", "Designer"]))
    .select(
        "*",
        F.count("employee_salary").over(w_ordered).alias("last4_count_employee_salary"),
        F.avg("employee_salary").over(w_ordered).alias("last4_avg_employee_salary"),
        F.max("employee_salary").over(w_ordered).alias("last4_max_employee_salary"),
        F.ntile(4).over(w2).alias("salary_bucket"),
    )
)

kept_keys = (
    enriched
    .select("job_title", "dept_id")
    .exceptAll(
        df_employees
        .filter(~F.col("is_remote"))
        .select("job_title", "dept_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

remaining_keys = (
    kept_rows
    .select("id")
    .subtract(
        df_employees
        .filter(F.col("level").isin(["L4", "L3"]))
        .select("id")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_employees = (
    verified
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "e.id",
        "e.full_name",
        "e.email",
        "e.dept_id",
        "e.manager_id",
        "e.job_title",
        "e.hire_date",
        "e.employee_level",
        "e.employee_salary",
        "e.last4_count_employee_salary",
        "e.last4_avg_employee_salary",
        "e.last4_max_employee_salary",
        F.col("d.salary").alias("employee_salary_2"),
        F.col("d.hire_date").alias("employee_hire_date"),
    )
)

daily = (
    enriched_employees
    .withColumn("year_start", F.date_trunc("YEAR", "employee_hire_date"))
    .cube("year_start", "manager_id")
    .agg(
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
        F.percentile_approx("employee_salary_2", 0.5).alias("employee_salary_2_p50"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list_2"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    daily
    .select(
        "distinct_manager_ids",
        "employee_salary_2_p50",
        "job_title_list",
        "job_title_list_2",
        "grouping_level",
    )
    .orderBy(F.col("grouping_level").desc())
)
