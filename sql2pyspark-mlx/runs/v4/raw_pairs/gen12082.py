from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L4")
    .groupBy(
        "job_title",
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        "is_remote",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
    )
    .orderBy("job_title")
)
