from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("budget").between(1413000, 3366000))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("NONE")))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title").isin(["Designer", "Analyst"])), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("budget").between(1312000, 3987000))
    .select(
        "id",
        "dept_name",
        "location",
        "dept_name_filled",
        F.greatest("budget_filled", "budget").alias("greatest_budget_filled_budget"),
        F.trim("location").alias("clean_location"),
    )
)

cleaned_departments = (
    cleaned
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.col("greatest_budget_filled_budget").cast("int").alias("greatest_budget_filled_budget_int"),
        F.concat_ws(",", "location", "dept_name").alias("location_dept_name_key"),
        F.coalesce("location", F.lit("N/A")).alias("location_clean"),
    )
)

result = (
    cleaned_departments
    .groupBy("dept_name")
    .pivot("location", ["London", "Bangkok"])
    .agg(F.sum("greatest_budget_filled_budget"))
    .select(
        "dept_name",
        F.col("London").alias("london"),
        F.col("Bangkok").alias("bangkok"),
    )
)
