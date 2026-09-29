from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-10-19").cast("date")) & F.col("full_name").isNull())
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.when(F.col("job_title") == "Manager", 1).otherwise(0).alias("is_manager"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

result = (
    cleaned
    .groupBy("manager_id")
    .pivot("job_title", ["Engineer", "Intern", "Analyst"])
    .agg(F.sum("is_manager").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "manager_id",
        F.col("Engineer_value_sum").alias("engineer_value_sum"),
        F.col("Engineer_n").alias("engineer_n"),
        F.col("Intern_value_sum").alias("intern_value_sum"),
        F.col("Intern_n").alias("intern_n"),
        F.col("Analyst_value_sum").alias("analyst_value_sum"),
        F.col("Analyst_n").alias("analyst_n"),
    )
)
