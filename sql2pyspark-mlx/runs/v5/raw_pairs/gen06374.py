from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@outlook.com"))
    .groupBy(
        "job_title",
        F.when(F.col("salary") >= 122000, "high").when(F.col("salary") >= 60000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .orderBy(F.col("salary_range").desc(), "job_title", "salary_bucket")
    .limit(3)
)
