from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("job_title").isin(["Engineer", "Analyst"]), "group_a").otherwise("other").alias("job_title_group"),
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
    )
    .filter(F.col("director_count") < 1)
)
