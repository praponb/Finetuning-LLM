from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("is_l1", "id")
w2 = Window.partitionBy("manager_id", "level")
w_ordered = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("employee_job_title", "manager_id").orderBy(F.col("budget").asc_nulls_last())

valid = (
    df_employees
    .filter(F.col("hire_date") < "2023-04-01")
    .withColumn("is_l1", F.when(F.col("level") == "L1", 1).otherwise(0))
)

with_windows = (
    valid
    .select(
        "*",
        F.lead("is_l1", 1).over(w).alias("next_is_l1"),
        F.max("salary").over(w2).alias("manager_id_max_salary"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.location"), F.col("p.dept_name"), F.col("p.budget"))
)

joined_employees = (
    joined
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .select("src.*", F.col("e.job_title").alias("employee_job_title"))
)

result = (
    joined_employees
    .withColumn("cumulative_manager_id_max_salary", F.sum("manager_id_max_salary").over(w_ordered))
    .withColumn("budget_percent_rank", F.percent_rank().over(w_running))
)
