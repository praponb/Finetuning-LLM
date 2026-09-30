from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w_hist = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

metrics = (
    df_departments
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("budget_percent_rank", F.percent_rank().over(w_hist))
    .withColumn("row_seq_2", F.row_number().over(w3))
    .withColumn("first_known_dept_name", F.first("dept_name", ignorenulls=True).over(w4))
    .withColumn("budget_avg_5", F.avg("budget").over(w5))
)

latest_match = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.level").alias("employee_level"),
    )
)

cleaned = (
    latest_match
    .filter(F.col("budget_avg_5").between(2462326, 3205503))
    .withColumn("location_employee_job_title", F.concat("location", F.lit(", "), "employee_job_title"))
    .withColumn("budget_avg_5_filled", F.coalesce("budget_avg_5", F.lit(0)))
)

result = (
    cleaned
    .groupBy(
        "employee_job_title",
        F.when(F.col("budget") >= 3569000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("employee_level")).alias("employee_level_list"),
        F.round(F.avg("budget_avg_5_filled"), 2).alias("avg_budget_avg_5_filled"),
    )
    .filter(F.col("avg_budget_avg_5_filled") < 0)
)
