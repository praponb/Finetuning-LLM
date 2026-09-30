from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("employee_salary", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("job_title").orderBy("salary", "id")
w3 = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id")
w4 = Window.partitionBy("manager_id").orderBy(F.col("event_seq_2").desc_nulls_last())

enriched = (
    df_employees
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.salary").alias("employee_salary"),
        F.col("p.full_name").alias("employee_full_name"),
        F.col("p.level").alias("employee_level"),
    )
)

valid = (
    enriched
    .filter(F.col("employee_level") == "L4")
    .select("*", F.upper("full_name").alias("full_name_upper"))
)

joined = (
    valid
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.hire_date").alias("employee_hire_date"))
)

with_windows = (
    joined
    .withColumn("employee_salary_avg_3", F.avg("employee_salary").over(w))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("event_seq_2", F.row_number().over(w3))
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

enriched_employees = (
    ranked
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.location"))
)

joined_employees = (
    enriched_employees
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.email").alias("employee_email"))
)

pivoted = (
    joined_employees
    .groupBy("dept_id", "level")
    .pivot("employee_level", ["L5", "L2", "L1"])
    .agg(F.sum("event_seq").alias("total"), F.count(F.lit(1)).alias("n"))
)

result = (
    pivoted
    .select("dept_id", "level", "L5_n", "L2_n", "L1_total", "L1_n")
    .orderBy(F.col("L1_n").desc(), F.col("dept_id").desc())
)
