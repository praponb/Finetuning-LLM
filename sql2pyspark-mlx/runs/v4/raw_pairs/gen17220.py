from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("QUARTER", "hire_date").alias("quarter_start"))
    .agg(
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Intern", 1)), F.lit(0)), 2).alias("salary_per_intern"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .orderBy(F.col("num_levels").asc_nulls_first())
)
