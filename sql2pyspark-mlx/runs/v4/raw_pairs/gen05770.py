from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("dept_name").orderBy(F.col("employee_salary").asc_nulls_last())
w4 = Window.partitionBy("dept_name", "location").orderBy(F.col("hire_date").asc_nulls_last(), "id")

ranked = (
    df_departments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"), F.col("c.hire_date"))
)

ranked_departments = (
    latest_match
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    ranked_departments
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        F.date_trunc("WEEK", "hire_date").alias("week_start"),
    )
    .agg(
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.percentile_approx("employee_salary", [0.25, 0.5, 0.75]).alias("employee_salary_quartiles"),
        F.round(F.avg("employee_salary"), 2).alias("avg_employee_salary"),
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
    )
    .orderBy(F.col("avg_budget").asc_nulls_last())
)
