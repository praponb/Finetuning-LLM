from pyspark.sql import functions as F

enriched = (
    df_employees
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.dept_name"), F.col("p.budget"), F.col("p.location"))
)

result = (
    enriched
    .groupBy(F.date_trunc("week", "hire_date").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_rows"),
        F.max("budget").alias("max_budget"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L5", 1)), F.lit(0)), 2).alias("salary_per_l5"),
        F.max(F.when(F.col("dept_name") == "HR", F.col("budget"))).alias("hr_max_budget"),
    )
    .orderBy("max_budget")
)
