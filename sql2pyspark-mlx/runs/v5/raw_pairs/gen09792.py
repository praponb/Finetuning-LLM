from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-07-24").cast("date")) & (F.col("job_title").eqNullSafe(F.lit("Director"))))
    .select(
        "*",
        F.lower("email").alias("email_lower"),
        F.coalesce("level", F.lit("OTHER")).alias("level_clean"),
    )
)

result = (
    base
    .withColumn("activity_month", F.date_trunc("MONTH", "hire_date"))
    .withColumn("salary_tier", F.when(F.col("salary") >= 125000, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("YEAR", "hire_date"))
    .groupingSets(
        [["activity_month", "salary_tier", "year_start"], ["activity_month", "salary_tier"], ["activity_month"]],
        "activity_month",
        "salary_tier",
        "year_start",
    )
    .agg(
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.grouping("activity_month").alias("grouping_level"),
    )
)
