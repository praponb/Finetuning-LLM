from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.col("manager_id").isNotNull())

level_group_summary = (
    valid_employees
    .groupBy(
        F.when(F.col("level").isin(["L5", "L4"]), "core").otherwise("other").alias("level_group"),
        F.year("hire_date").alias("hire_year"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0)).alias("manager_count"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
    )
)

result = (
    level_group_summary
    .filter(F.col("manager_count") >= 74)
    .select("level_group", "hire_year", "manager_count", "pct_designer")
    .orderBy(F.col("manager_count").desc(), "level_group", "hire_year")
    .limit(10)
)
