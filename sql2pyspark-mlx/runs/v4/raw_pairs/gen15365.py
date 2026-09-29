from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id")
w2 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("job_title")
w5 = Window.orderBy("budget", "id")
w6 = Window.orderBy("salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("job_title").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("location").orderBy("salary", "id").rowsBetween(-6, -1)
w9 = Window.orderBy("first_known_salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("salary", "id").rowsBetween(-5, Window.currentRow)
w11 = Window.partitionBy("manager_id").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("location", "dept_id").orderBy("salary_share_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("job_title", "manager_id").orderBy("salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.partitionBy("dept_name", "dept_id").orderBy("cumulative_budget", "id").rowsBetween(-5, -1)

enriched = (
    df_employees
    .withColumn("level_max_salary", F.max("salary").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("salary_avg_6", F.avg("salary").over(w3))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w4)), 4))
)

enriched_employees = (
    enriched
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.dept_name"), F.col("p.location"), F.col("p.budget"))
)

joined = (
    enriched_employees
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.salary").alias("employee_salary"))
)

metrics = (
    joined
    .withColumn("budget_bucket", F.ntile(10).over(w5))
    .withColumn("cumulative_budget", F.sum("budget").over(w6))
    .withColumn("first_known_salary_share", F.first("salary_share", ignorenulls=True).over(w7))
)

enriched_employees_2 = (
    metrics
    .select(
        "*",
        F.avg("salary_share").over(w8).alias("salary_share_avg_6_prior"),
        F.sum("employee_salary").over(w9).alias("running_employee_salary"),
        F.avg("salary_avg_6").over(w10).alias("salary_avg_6_avg_5"),
        F.first("salary_share", ignorenulls=True).over(w11).alias("first_known_salary_share_2"),
    )
)

enriched_employees_3 = (
    enriched_employees_2
    .select(
        "*",
        F.first("salary_share", ignorenulls=True).over(w12).alias("first_known_salary_share_3"),
        F.sum("salary_avg_6").over(w13).alias("cumulative_salary_avg_6"),
        F.avg("salary_avg_6").over(w14).alias("salary_avg_6_avg_5_prior"),
    )
)

result = (
    enriched_employees_3
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "is_remote",
        "level",
        "level_max_salary",
        "event_seq",
        "budget",
    )
    .orderBy(F.col("level").asc_nulls_last(), F.col("id").desc())
    .limit(5)
)
