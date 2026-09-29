from pyspark.sql import functions as F

joined = (
    df_employees
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.location"), F.col("d.budget"))
)

result = (
    joined
    .groupBy(
        "level",
        F.when(F.col("budget") >= 3383000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("job_title") == "Engineer", 1)), F.lit(0)), 2).alias("budget_per_engineer"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)
