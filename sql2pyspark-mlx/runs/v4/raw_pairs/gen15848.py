from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("department_budget", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w3 = Window.partitionBy("manager_id").orderBy(F.col("department_budget").asc_nulls_last())
w4 = Window.partitionBy("dept_name")
w5 = Window.partitionBy("quarter_start").orderBy(F.col("london_events").desc())

with_dims = (
    df_employees
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.dept_name"), F.col("p.budget"))
)

with_dims_employees = (
    with_dims
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.budget").alias("department_budget"), F.col("p.location"))
)

metrics = (
    with_dims_employees
    .withColumn("salary_avg_3", F.avg("salary").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("department_budget_percent_rank", F.percent_rank().over(w3))
    .withColumn("dept_name_count_salary", F.count("salary").over(w4))
)

result = (
    metrics
    .groupBy(
        F.date_trunc("quarter", "hire_date").alias("quarter_start"),
        F.when(F.col("department_budget_percent_rank") >= 0.68, "PREMIUM").otherwise("STANDARD").alias("department_budget_percent_rank_tier"),
    )
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.count_if(F.col("location") == "London").alias("london_events"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
        F.round(F.sum("dept_name_count_salary") / F.nullif(F.count(F.when(F.col("location") == "Bangkok", 1)), F.lit(0)), 2).alias("dept_name_count_salary_per_bangkok"),
    )
    .withColumn("london_events_rank", F.rank().over(w5))
    .orderBy("distinct_dept_names")
)
