from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id", "job_title")
w3 = Window.partitionBy("job_title").orderBy("hire_date", "id")
w4 = Window.partitionBy("dept_id").orderBy("cumulative_salary", "id")
w_ordered = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_employees
    .select(
        "*",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    deduped
    .select(
        "*",
        F.lag("salary_share", 1).over(w4).alias("prev_salary_share"),
        F.avg("cumulative_salary").over(w_ordered).alias("cumulative_salary_avg_5"),
        F.first("job_title", ignorenulls=True).over(w6).alias("first_known_job_title"),
    )
)

enriched_employees = (
    enriched
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.location"), F.col("b.dept_name"))
)

enriched_employees_2 = (
    enriched_employees
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.level").alias("employee_level"))
)

result = (
    enriched_employees_2
    .groupBy("dept_id", "employee_level")
    .pivot("job_title", ["Analyst", "Manager"])
    .agg(F.avg("salary_share").alias("value_sum"))
    .select(
        "dept_id",
        "employee_level",
        F.col("Analyst").alias("analyst"),
        F.col("Manager").alias("manager"),
    )
)
